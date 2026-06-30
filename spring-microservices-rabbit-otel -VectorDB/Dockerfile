# ── Stage 1: Build all Maven modules ─────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy parent POM first so dependency layer is cached
COPY pom.xml .

# Copy each module POM (cache Maven deps before source copy)
COPY common/pom.xml           common/pom.xml
COPY catalog-service/pom.xml  catalog-service/pom.xml
COPY order-service/pom.xml    order-service/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY payment-service/pom.xml  payment-service/pom.xml
COPY shipment-service/pom.xml shipment-service/pom.xml

RUN mvn dependency:go-offline -B

# Copy all source code
COPY common/src           common/src
COPY catalog-service/src  catalog-service/src
COPY order-service/src    order-service/src
COPY inventory-service/src inventory-service/src
COPY payment-service/src  payment-service/src
COPY shipment-service/src shipment-service/src

RUN mvn clean package -DskipTests -B

# ── Stage 2: Minimal runtime image ────────────────────────────────────────────
FROM eclipse-temurin:21-jre-jammy

ARG SERVICE_NAME
ENV SERVICE_NAME=${SERVICE_NAME}

WORKDIR /app

# Copy the specific service JAR
COPY --from=build /app/${SERVICE_NAME}/target/${SERVICE_NAME}-*.jar app.jar

# Copy the OTel Java agent (must exist at root of repo)
COPY opentelemetry-javaagent.jar otel-agent.jar

# OTel agent is injected via JAVA_TOOL_OPTIONS so it is always active
ENV JAVA_TOOL_OPTIONS="-javaagent:/app/otel-agent.jar"

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]
