#!/usr/bin/env bash
#
# Load generator for the OtelToVectorDB microservices demo.
# Drives mixed read/write traffic across catalog, order, inventory, payment
# and shipment services so OTel traces/metrics/logs get produced end to end.
#
# Produces order failures in three distinct, categorizable ways:
#   - payment decline, with a varied reason (see payment-service PAYMENT_FAILURE_RATE)
#   - inventory: product not found (bogus ULID, via action_error_traffic)
#   - inventory: insufficient stock (oversized quantity, via action_create_oversized_order)
#
# Usage:
#   ./load-test.sh
#   DURATION=600 CONCURRENCY=10 ERROR_RATE=15 ./load-test.sh
#
# Env vars (all optional):
#   DURATION           total run time in seconds        (default 300)
#   CONCURRENCY        parallel workers                 (default 5)
#   REQUEST_DELAY      sleep between a worker's requests (default 0.5)
#   NUM_SEED_PRODUCTS  min products to have in catalog   (default 5)
#   SEED_STOCK         inventory qty seeded per product  (default 500)
#   ERROR_RATE         % of requests that deliberately hit bad data (default 10)
#   LOG_DIR            where to write the run log        (default ./load-test-logs)
#
# Requires: bash, curl, python3 (for tiny JSON field extraction).

set -uo pipefail

CATALOG_URL="http://localhost:8100"
ORDER_URL="http://localhost:8102"
INVENTORY_URL="http://localhost:8101"
PAYMENT_URL="http://localhost:8103"
SHIPMENT_URL="http://localhost:8104"

DURATION=${DURATION:-300}
CONCURRENCY=${CONCURRENCY:-5}
REQUEST_DELAY=${REQUEST_DELAY:-0.5}
NUM_SEED_PRODUCTS=${NUM_SEED_PRODUCTS:-5}
SEED_STOCK=${SEED_STOCK:-500}
ERROR_RATE=${ERROR_RATE:-10}
LOG_DIR=${LOG_DIR:-./load-test-logs}

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/load-$(date +%Y%m%d-%H%M%S).log"
PRODUCTS_FILE=$(mktemp)
ORDERS_FILE=$(mktemp)

cleanup() {
    log "Stopping load generator..."
    jobs -p | xargs -r kill 2>/dev/null
    rm -f "$PRODUCTS_FILE" "$ORDERS_FILE"
}
trap cleanup EXIT INT TERM

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S.%3N') $*" | tee -a "$LOG_FILE"
}

# Perform an HTTP request, log status+latency, print the response body to stdout.
req() {
    local method="$1" url="$2" data="${3:-}" label="$4"
    local t0 t1 ms raw code body
    t0=$(date +%s%N)
    if [ -n "$data" ]; then
        raw=$(curl -s -w $'\n%{http_code}' -X "$method" "$url" \
              -H "Content-Type: application/json" -d "$data")
    else
        raw=$(curl -s -w $'\n%{http_code}' -X "$method" "$url")
    fi
    t1=$(date +%s%N)
    ms=$(( (t1 - t0) / 1000000 ))
    code=$(echo "$raw" | tail -n1)
    body=$(echo "$raw" | sed '$d')
    echo "$(date '+%H:%M:%S.%3N') [$label] $method $url -> $code (${ms}ms)" >> "$LOG_FILE"
    printf '%s' "$body"
}

extract() {
    # $1 = python expression referencing `d`, JSON read from stdin
    python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    v = ($1)
    if v is not None:
        print(v)
except Exception:
    pass
" 2>/dev/null
}

pick_random_line() {
    local file="$1" n
    n=$(wc -l < "$file" 2>/dev/null || echo 0)
    [ "${n:-0}" -eq 0 ] && return 1
    sed -n "$(( (RANDOM % n) + 1 ))p" "$file"
}

# ---------- seeding ----------

seed_products() {
    log "Checking existing catalog products..."
    local count
    count=$(curl -s "$CATALOG_URL/api/products" | extract "len(d)")
    count=${count:-0}
    if [ "$count" -lt "$NUM_SEED_PRODUCTS" ]; then
        log "Seeding products (found $count, want at least $NUM_SEED_PRODUCTS)..."
        local names=(Widget Gadget Gizmo Doohickey Thingamajig Contraption Module Component Accessory Device)
        local i
        for (( i = 0; i < NUM_SEED_PRODUCTS; i++ )); do
            local name="${names[$((RANDOM % ${#names[@]}))]}-$RANDOM"
            local price=$(( (RANDOM % 500) + 10 ))
            req POST "$CATALOG_URL/api/products" \
                "{\"name\":\"$name\",\"price\":$price.99}" "seed_product" > /dev/null
        done
    fi
    curl -s "$CATALOG_URL/api/products" | extract "'\n'.join(p['ulid'] for p in d)" > "$PRODUCTS_FILE"
    log "Product pool ready: $(wc -l < "$PRODUCTS_FILE") products"
}

seed_inventory() {
    log "Ensuring inventory stock for all products..."
    local ulid status
    while read -r ulid; do
        [ -z "$ulid" ] && continue
        status=$(curl -s -o /dev/null -w "%{http_code}" "$INVENTORY_URL/api/inventory/$ulid")
        if [ "$status" != "200" ]; then
            req POST "$INVENTORY_URL/api/inventory" \
                "{\"productUlid\":\"$ulid\",\"availableQuantity\":$SEED_STOCK,\"reservedQuantity\":0}" \
                "seed_inventory" > /dev/null
        fi
    done < "$PRODUCTS_FILE"
    log "Inventory seeding complete."
}

# ---------- traffic actions ----------

action_create_order() {
    local num_items=$(( (RANDOM % 3) + 1 ))
    local items_json="" ulid qty i
    for (( i = 0; i < num_items; i++ )); do
        ulid=$(pick_random_line "$PRODUCTS_FILE") || continue
        qty=$(( (RANDOM % 5) + 1 ))
        items_json+="{\"productUlid\":\"$ulid\",\"quantity\":$qty},"
    done
    [ -z "$items_json" ] && return
    local payload="{\"items\":[${items_json%,}]}"
    local resp order_ulid
    resp=$(req POST "$ORDER_URL/api/orders" "$payload" "create_order")
    order_ulid=$(echo "$resp" | extract "d.get('ulid')")
    [ -n "$order_ulid" ] && echo "$order_ulid" >> "$ORDERS_FILE"
}

# Deliberately orders more than the seeded stock of one product so
# inventory-service's isInStock() check fails deterministically — a real
# "insufficient stock" failure, distinct from the "product not found"
# failures produced by action_error_traffic's bogus-ULID orders.
action_create_oversized_order() {
    local ulid qty
    ulid=$(pick_random_line "$PRODUCTS_FILE") || return
    qty=$(( SEED_STOCK + (RANDOM % 200) + 50 ))
    local payload="{\"items\":[{\"productUlid\":\"$ulid\",\"quantity\":$qty}]}"
    local resp order_ulid
    resp=$(req POST "$ORDER_URL/api/orders" "$payload" "create_oversized_order")
    order_ulid=$(echo "$resp" | extract "d.get('ulid')")
    [ -n "$order_ulid" ] && echo "$order_ulid" >> "$ORDERS_FILE"
}

action_list_products()   { req GET "$CATALOG_URL/api/products" "" "list_products" > /dev/null; }
action_get_product()     { local u; u=$(pick_random_line "$PRODUCTS_FILE") && req GET "$CATALOG_URL/api/products/$u" "" "get_product" > /dev/null; }
action_list_inventory()  { req GET "$INVENTORY_URL/api/inventory" "" "list_inventory" > /dev/null; }
action_get_inventory()   { local u; u=$(pick_random_line "$PRODUCTS_FILE") && req GET "$INVENTORY_URL/api/inventory/$u" "" "get_inventory" > /dev/null; }
action_list_payments()   { req GET "$PAYMENT_URL/api/payments" "" "list_payments" > /dev/null; }
action_get_payment()     { local o; o=$(pick_random_line "$ORDERS_FILE") && req GET "$PAYMENT_URL/api/payments/$o" "" "get_payment" > /dev/null; }
action_list_shipments()  { req GET "$SHIPMENT_URL/api/shipments" "" "list_shipments" > /dev/null; }
action_get_shipment()    { local o; o=$(pick_random_line "$ORDERS_FILE") && req GET "$SHIPMENT_URL/api/shipments/$o" "" "get_shipment" > /dev/null; }

# Deliberate bad requests: exercises error handling / produces error spans & logs.
action_error_traffic() {
    local bad="INVALID-$RANDOM$RANDOM"
    case $(( RANDOM % 4 )) in
        0) req GET  "$CATALOG_URL/api/products/$bad" "" "error_get_product" > /dev/null ;;
        1) req GET  "$INVENTORY_URL/api/inventory/$bad" "" "error_get_inventory" > /dev/null ;;
        2) req GET  "$PAYMENT_URL/api/payments/$bad" "" "error_get_payment" > /dev/null ;;
        3) req POST "$ORDER_URL/api/orders" \
               "{\"items\":[{\"productUlid\":\"$bad\",\"quantity\":1}]}" "error_bad_order" > /dev/null ;;
    esac
}

# weighted action pool: name:weight
ACTIONS_WEIGHTED=(
    "action_create_order:20"
    "action_create_oversized_order:8"
    "action_list_products:15"
    "action_get_product:15"
    "action_list_inventory:10"
    "action_get_inventory:10"
    "action_list_payments:8"
    "action_get_payment:8"
    "action_list_shipments:7"
    "action_get_shipment:7"
)

build_action_pool() {
    ACTION_POOL=()
    local spec name weight w
    for spec in "${ACTIONS_WEIGHTED[@]}"; do
        name="${spec%%:*}"
        weight="${spec##*:}"
        for (( w = 0; w < weight; w++ )); do
            ACTION_POOL+=("$name")
        done
    done
}

worker() {
    local worker_id="$1" end_time="$2"
    RANDOM=$(( BASHPID + worker_id ))  # decorrelate PRNG across forked workers
    while [ "$(date +%s)" -lt "$end_time" ]; do
        if (( RANDOM % 100 < ERROR_RATE )); then
            action_error_traffic
        else
            "${ACTION_POOL[$((RANDOM % ${#ACTION_POOL[@]}))]}"
        fi
        sleep "$REQUEST_DELAY"
    done
}

print_summary() {
    log "=== Summary ==="
    log "Total requests logged : $(grep -c '\->' "$LOG_FILE" 2>/dev/null || echo 0)"
    log "Orders created         : $(wc -l < "$ORDERS_FILE" 2>/dev/null || echo 0)"
    log "Non-2xx responses      : $(grep -Ec -- '-> [345][0-9]{2}' "$LOG_FILE" 2>/dev/null || echo 0)"
    log "Full log               : $LOG_FILE"
}

main() {
    log "=== Load generator starting ==="
    log "duration=${DURATION}s concurrency=${CONCURRENCY} request_delay=${REQUEST_DELAY}s error_rate=${ERROR_RATE}%"

    for svc in "catalog:$CATALOG_URL/actuator/health" "order:$ORDER_URL/actuator/health" \
               "inventory:$INVENTORY_URL/actuator/health" "payment:$PAYMENT_URL/actuator/health" \
               "shipment:$SHIPMENT_URL/actuator/health"; do
        name="${svc%%:*}"; url="${svc#*:}"
        code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "$url")
        if [ "$code" != "200" ]; then
            log "WARNING: $name-service not healthy (HTTP $code) at $url — is docker compose up?"
        fi
    done

    seed_products
    seed_inventory
    build_action_pool

    local end_time=$(( $(date +%s) + DURATION ))
    log "Spawning $CONCURRENCY workers for ${DURATION}s..."
    for (( i = 1; i <= CONCURRENCY; i++ )); do
        worker "$i" "$end_time" &
    done
    wait
    print_summary
    log "=== Load generator finished ==="
}

main "$@"
