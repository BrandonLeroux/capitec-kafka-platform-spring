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
    private final String paymentFailedTopic;
    private final String inventoryTopic;
    private final String orderServiceUrl;

    public FulfilmentService(KafkaTemplate<String, String> kafka,
                             @Value("${app.topics.order}") String orderTopic,
                             @Value("${app.topics.payment-failed:order-cancelled}") String paymentFailedTopic,
                             @Value("${app.topics.inventory:inventory}") String inventoryTopic,
                             @Value("${app.order-service.url:http://spring-order-service:8081}") String orderServiceUrl) {
        this.kafka              = kafka;
        this.orderTopic         = orderTopic;
        this.paymentFailedTopic = paymentFailedTopic;
        this.inventoryTopic     = inventoryTopic;
        this.orderServiceUrl    = orderServiceUrl;
    }

    @Async
    public void process(String orderID, String customerID, String product, double amount, int qty) {
        try {
            log.info("Processing payment orderID={}", orderID);

            // ── Step 1: Payment processing (2s) ──────────────────────────────
            // This is where payment.succeeded or payment.failed is determined.
            // Inventory was already deducted at checkout (CONFIRMED status).
            sleep(2_000);

            if (isCancelled(orderID)) {
                log.info("Order cancelled before payment processed — stopping orderID={}", orderID);
                return;
            }

            // Simulate payment failure (~10% of orders fail)
            if (Math.random() < 0.10) {
                publishPaymentFailed(orderID, customerID, product, amount, qty);
                return;
            }

            // ── Payment succeeded → continue fulfilment pipeline ──────────────
            publish(orderID, customerID, product, amount, "PAYMENT-PROCESSED");

            sleep(3_000);
            if (isCancelled(orderID)) { log.info("Cancelled before PACKED orderID={}", orderID); return; }
            publish(orderID, customerID, product, amount, "PACKED");

            sleep(3_000);
            if (isCancelled(orderID)) { log.info("Cancelled before OUT-FOR-DELIVERY orderID={}", orderID); return; }
            publish(orderID, customerID, product, amount, "OUT-FOR-DELIVERY");

            sleep(5_000);
            if (isCancelled(orderID)) { log.info("Cancelled before DELIVERED orderID={}", orderID); return; }
            publish(orderID, customerID, product, amount, "DELIVERED");

            log.info("Order fulfilled orderID={}", orderID);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("FulfilmentService interrupted orderID={}", orderID);
        }
    }

    /**
     * Payment failed flow:
     * 1. Publish CANCELLED to order topic so order-service updates DB status
     * 2. Publish cancellation event to order-cancelled topic (for audit)
     * 3. Restore inventory — stock WAS deducted at checkout so must be returned
     */
    private void publishPaymentFailed(String orderID, String customerID, String product,
                                      double amount, int qty) {
        log.warn("Payment FAILED for orderID={} — cancelling order and restoring inventory", orderID);

        // 1. Update order status to CANCELLED via order-created topic
        String cancelPayload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\",\"amount\":%.2f," +
            "\"status\":\"CANCELLED\",\"updatedAt\":\"%s\"}",
            orderID, customerID, product, amount, LocalDateTime.now());
        kafka.send(orderTopic, orderID, cancelPayload);

        // 2. Publish to order-cancelled topic with reason = payment failure
        String cancelledPayload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"reason\":\"Payment failed\",\"cancelledAt\":\"%s\"}",
            orderID, customerID, LocalDateTime.now());
        kafka.send(paymentFailedTopic, orderID, cancelledPayload);

        // 3. Restore inventory — inventory was deducted when order was CONFIRMED.
        //    Payment failure = order never fulfilled, so stock must be returned.
        if (product != null && !product.isBlank()) {
            String invPayload = String.format(
                "{\"productID\":\"%s\",\"quantity\":%d,\"action\":\"ADJUST\",\"orderID\":\"%s\",\"reason\":\"payment-failed\"}",
                product, qty, orderID);
            kafka.send(inventoryTopic, product, invPayload)
                 .whenComplete((r, ex) -> {
                     if (ex != null) log.error("Failed to restore inventory after payment failure orderID={}", orderID, ex);
                     else log.info("Inventory restored product={} qty=+{} after payment failure orderID={}", product, qty, orderID);
                 });
        }

        log.warn("Payment failure flow complete orderID={}", orderID);
    }

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
            log.warn("Could not check cancellation for orderID={} — continuing: {}", orderID, e.getMessage());
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
