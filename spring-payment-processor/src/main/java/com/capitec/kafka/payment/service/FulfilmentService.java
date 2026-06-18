package com.capitec.kafka.payment.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class FulfilmentService {

    private static final Logger log = LoggerFactory.getLogger(FulfilmentService.class);

    private final KafkaTemplate<String, String> kafka;
    private final String orderTopic;

    public FulfilmentService(KafkaTemplate<String, String> kafka,
                             @Value("${app.topics.order}") String orderTopic) {
        this.kafka      = kafka;
        this.orderTopic = orderTopic;
    }

    @Async
    public void process(String orderID, String customerID, String product, double amount) {
        try {
            log.info("Processing payment orderID={}", orderID);
            sleep(2_000); publish(orderID, customerID, product, amount, "PAYMENT-PROCESSED");
            sleep(3_000); publish(orderID, customerID, product, amount, "PACKED");
            sleep(3_000); publish(orderID, customerID, product, amount, "OUT-FOR-DELIVERY");
            sleep(5_000); publish(orderID, customerID, product, amount, "DELIVERED");
            log.info("Order fulfilled orderID={}", orderID);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("FulfilmentService interrupted orderID={}", orderID);
        }
    }

    private void publish(String orderID, String customerID, String product, double amount, String status) {
        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\",\"amount\":%.2f,\"status\":\"%s\",\"updatedAt\":\"%s\"}",
            orderID, customerID, product, amount, status, LocalDateTime.now());
        kafka.send(orderTopic, orderID, payload)
             .whenComplete((r, ex) -> {
                 if (ex != null) log.error("Failed to publish status orderID={} status={}", orderID, status, ex);
                 else log.info("Status published orderID={} status={}", orderID, status);
             });
    }

    private void sleep(long ms) throws InterruptedException { Thread.sleep(ms); }
}
