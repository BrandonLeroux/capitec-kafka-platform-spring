package com.capitec.kafka.payment.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.Map;

@Service
public class FulfilmentService {

    private static final Logger log = LoggerFactory.getLogger(FulfilmentService.class);

    private final KafkaTemplate<String, String> kafka;
    private final RestTemplate restTemplate = new RestTemplate();
    private final String orderTopic;
    private final String orderServiceUrl;

    public FulfilmentService(KafkaTemplate<String, String> kafka,
                             @Value("${app.topics.order}") String orderTopic,
                             @Value("${app.order-service.url:http://spring-order-service:8081}") String orderServiceUrl) {
        this.kafka           = kafka;
        this.orderTopic      = orderTopic;
        this.orderServiceUrl = orderServiceUrl;
    }

    @Async
    public void process(String orderID, String customerID, String product, double amount) {
        try {
            log.info("Processing payment orderID={}", orderID);

            sleep(2_000);
            if (isCancelled(orderID)) { log.info("Order cancelled before PAYMENT-PROCESSED — stopping orderID={}", orderID); return; }
            publish(orderID, customerID, product, amount, "PAYMENT-PROCESSED");

            sleep(3_000);
            if (isCancelled(orderID)) { log.info("Order cancelled before PACKED — stopping orderID={}", orderID); return; }
            publish(orderID, customerID, product, amount, "PACKED");

            sleep(3_000);
            if (isCancelled(orderID)) { log.info("Order cancelled before OUT-FOR-DELIVERY — stopping orderID={}", orderID); return; }
            publish(orderID, customerID, product, amount, "OUT-FOR-DELIVERY");

            sleep(5_000);
            if (isCancelled(orderID)) { log.info("Order cancelled before DELIVERED — stopping orderID={}", orderID); return; }
            publish(orderID, customerID, product, amount, "DELIVERED");

            log.info("Order fulfilled orderID={}", orderID);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("FulfilmentService interrupted orderID={}", orderID);
        }
    }

    // Check order-service DB for CANCELLED status before each step
    private boolean isCancelled(String orderID) {
        try {
            var response = restTemplate.getForObject(
                orderServiceUrl + "/api/orders?search=" + orderID + "&size=1", Map.class);
            if (response == null) return false;
            var orders = (java.util.List<?>) response.get("orders");
            if (orders == null || orders.isEmpty()) return false;
            var order = (Map<?, ?>) orders.get(0);
            return "CANCELLED".equals(order.get("status"));
        } catch (Exception e) {
            log.warn("Could not check cancellation status for orderID={} — continuing: {}", orderID, e.getMessage());
            return false;
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
