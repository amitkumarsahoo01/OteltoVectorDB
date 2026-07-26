package com.datmt.learning.java.payment.service;

import com.datmt.learning.java.common.dto.OrderPlacedEvent;
import com.datmt.learning.java.common.dto.PaymentFailedEvent;
import com.datmt.learning.java.common.dto.PaymentProcessedEvent;
import com.datmt.learning.java.common.helper.MessagingTopics;
import com.datmt.learning.java.payment.model.Payment;
import com.datmt.learning.java.payment.model.PaymentStatus;
import com.datmt.learning.java.payment.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PaymentService {
    private final Logger log = LoggerFactory.getLogger(PaymentService.class);

    // Distinct, categorizable decline reasons — real payment gateways fail for
    // more than one reason, and telemetry/RAG queries need that variety to
    // group failures meaningfully instead of seeing one generic message.
    private static final String[] FAILURE_REASONS = {
            "Card declined",
            "Insufficient funds",
            "Card expired",
            "Payment gateway timeout",
            "Fraud check failed",
            "Invalid CVV",
    };

    private final PaymentRepository repo;
    private final RabbitTemplate rabbitTemplate;

    @Value("${payment.failure-rate:0.4}")
    private double failureRate;

    public PaymentService(PaymentRepository repo, RabbitTemplate rabbitTemplate) {
        this.repo = repo;
        this.rabbitTemplate = rabbitTemplate;
    }

    public List<Payment> getAll() {
        return repo.findAll();
    }

    public Payment getByOrderUlid(String orderUlid) {
        return repo.findByOrderUlid(orderUlid)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    public void processOrder(OrderPlacedEvent event) {
        log.info("PaymentService.processOrder {}", event);
        BigDecimal totalAmount = mockAmountCalculation(event); // In real world, this is calculated per item

        Payment payment = new Payment();
        payment.setOrderUlid(event.orderUlid());
        payment.setAmount(totalAmount);
        payment.setStatus(PaymentStatus.PENDING);

        boolean success = ThreadLocalRandom.current().nextDouble() >= failureRate;

        if (success) {
            payment.setStatus(PaymentStatus.SUCCESS);
            repo.save(payment);

            PaymentProcessedEvent processed = new PaymentProcessedEvent(
                    event.orderUlid(),
                    totalAmount
            );
            log.info("Payment success: {}", processed);
            rabbitTemplate.convertAndSend(
                    MessagingTopics.Payment.EXCHANGE,
                    MessagingTopics.Payment.ROUTING_KEY_PAYMENT_PROCESSED,
                    processed
            );

        } else {
            payment.setStatus(PaymentStatus.FAILED);
            payment.setFailureReason(FAILURE_REASONS[ThreadLocalRandom.current().nextInt(FAILURE_REASONS.length)]);
            repo.save(payment);

            PaymentFailedEvent failed = new PaymentFailedEvent(
                    event.orderUlid(),
                    payment.getFailureReason()
            );
            log.info("Payment failed: {}", failed);
            rabbitTemplate.convertAndSend(
                    MessagingTopics.Payment.EXCHANGE,
                    MessagingTopics.Payment.ROUTING_KEY_PAYMENT_FAILED,
                    failed
            );
        }
    }

    private BigDecimal mockAmountCalculation(OrderPlacedEvent event) {
        return BigDecimal.valueOf(event.items().size() * 10.0); // stubbed
    }
}

