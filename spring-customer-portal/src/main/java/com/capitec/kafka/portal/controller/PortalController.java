package com.capitec.kafka.portal.controller;

import com.capitec.kafka.portal.model.CustomerSession;
import com.capitec.kafka.portal.service.SequenceService;
import jakarta.servlet.http.HttpSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Map;

@RestController
public class PortalController {

    private static final Logger log = LoggerFactory.getLogger(PortalController.class);

    private final KafkaTemplate<String, String> kafka;
    private final SequenceService sequenceService;
    private final RestTemplate    restTemplate;
    private final String orderTopic;
    private final String customerTopic;
    private final String cancelledTopic;
    private final String inventoryTopic;
    private final String orderServiceUrl;

    public PortalController(KafkaTemplate<String, String> kafka,
                            SequenceService sequenceService,
                            @Value("${app.topics.order}")     String orderTopic,
                            @Value("${app.topics.customer}")  String customerTopic,
                            @Value("${app.topics.cancelled}") String cancelledTopic,
                            @Value("${app.topics.inventory}") String inventoryTopic,
                            @Value("${app.order-service.url}") String orderServiceUrl) {
        this.kafka           = kafka;
        this.sequenceService = sequenceService;
        this.restTemplate    = new RestTemplate();
        this.orderTopic      = orderTopic;
        this.customerTopic   = customerTopic;
        this.cancelledTopic  = cancelledTopic;
        this.inventoryTopic  = inventoryTopic;
        this.orderServiceUrl = orderServiceUrl;
    }

    @GetMapping("/")
    public ResponseEntity<String> ui() {
        return ResponseEntity.ok().contentType(MediaType.TEXT_HTML).body(PortalHtml.build());
    }

    // ── Login ─────────────────────────────────────────────────────────────────
    @PostMapping("/api/login")
    public ResponseEntity<?> login(@RequestBody Map<String, String> body, HttpSession session) {
        String identifier = body.get("cell");
        String password   = body.get("password");
        if (identifier == null || password == null)
            return ResponseEntity.badRequest().body(Map.of("error", "cell and password required"));

        try {
            var resp = restTemplate.getForObject(
                orderServiceUrl + "/api/customer/by-identifier?q=" + identifier, Map.class);
            if (resp == null) return ResponseEntity.status(401).body(Map.of("error", "Customer not found"));

            String stored = (String) resp.getOrDefault("passwordHash", resp.get("password_hash"));
            if (stored == null || !stored.equals(sha256(password)))
                return ResponseEntity.status(401).body(Map.of("error", "Invalid password"));

            String customerID = (String) resp.getOrDefault("customerID", resp.get("customerId"));
            long   custNum    = resp.get("customerNumber") instanceof Number n ? n.longValue() : 0L;
            String firstName  = (String) resp.getOrDefault("firstName", "");

            var s = new CustomerSession(customerID, custNum, firstName, identifier);
            session.setAttribute("customer", s);

            log.info("Login success customerID={}", customerID);
            return ResponseEntity.ok(Map.of("customerID", customerID, "customerNumber", custNum, "firstName", firstName));
        } catch (Exception e) {
            return ResponseEntity.status(401).body(Map.of("error", "Customer not found"));
        }
    }

    // ── Register ──────────────────────────────────────────────────────────────
    @PostMapping("/api/register")
    public ResponseEntity<?> register(@RequestBody Map<String, String> body) {
        String firstName = body.get("firstName");
        String lastName  = body.get("lastName");
        String cell      = body.get("cell");
        String password  = body.get("password");
        if (firstName == null || cell == null || password == null)
            return ResponseEntity.badRequest().body(Map.of("error", "All fields required"));

        try {
            long custNum   = sequenceService.next();
            String custID  = String.valueOf(custNum);
            String hash    = sha256(password);
            String payload = String.format(
                "{\"customerID\":\"%s\",\"customerNumber\":%d,\"firstName\":\"%s\",\"lastName\":\"%s\"," +
                "\"idNumber\":\"%s\",\"email\":\"%s\",\"cell\":\"%s\",\"passwordHash\":\"%s\"}",
                custID, custNum, esc(firstName), esc(lastName != null ? lastName : ""),
                esc(body.getOrDefault("idNumber", "")), esc(body.getOrDefault("email", "")),
                esc(cell), hash);
            kafka.send(customerTopic, custID, payload);
            log.info("Registered customerID={} number={}", custID, custNum);
            return ResponseEntity.ok(Map.of("customerID", custID, "customerNumber", custNum));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    // ── Logout ────────────────────────────────────────────────────────────────
    @PostMapping("/api/logout")
    public ResponseEntity<?> logout(HttpSession session) {
        session.invalidate();
        return ResponseEntity.ok(Map.of("ok", true));
    }

    // ── Profile ───────────────────────────────────────────────────────────────
    @GetMapping("/api/me")
    public ResponseEntity<?> me(HttpSession session) {
        var s = (CustomerSession) session.getAttribute("customer");
        if (s == null) return ResponseEntity.status(401).body(Map.of("error", "Not logged in"));
        try {
            var resp = restTemplate.getForObject(orderServiceUrl + "/api/customer/" + s.customerID, Map.class);
            return resp != null ? ResponseEntity.ok(resp) : ResponseEntity.notFound().build();
        } catch (Exception e) { return ResponseEntity.notFound().build(); }
    }

    @PutMapping("/api/profile")
    public ResponseEntity<?> updateProfile(@RequestBody Map<String, Object> body, HttpSession session) {
        var s = (CustomerSession) session.getAttribute("customer");
        if (s == null) return ResponseEntity.status(401).body(Map.of("error", "Not logged in"));
        try {
            restTemplate.postForObject(orderServiceUrl + "/api/customer/register", body, Map.class);
            return ResponseEntity.ok(Map.of("ok", true));
        } catch (Exception e) { return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage())); }
    }

    // ── My orders ─────────────────────────────────────────────────────────────
    @GetMapping("/api/my-orders")
    public ResponseEntity<?> myOrders(HttpSession session) {
        var s = (CustomerSession) session.getAttribute("customer");
        if (s == null) return ResponseEntity.status(401).body(Map.of("error", "Not logged in"));
        try {
            var resp = restTemplate.getForObject(
                orderServiceUrl + "/api/orders?customerID=" + s.customerID + "&size=50", Map.class);
            return ResponseEntity.ok(resp != null ? resp : Map.of("orders", new Object[0]));
        } catch (Exception e) { return ResponseEntity.ok(Map.of("orders", new Object[0])); }
    }

    // ── Submit order ──────────────────────────────────────────────────────────
    @PostMapping("/api/order")
    public ResponseEntity<?> order(@RequestBody Map<String, Object> body, HttpSession session) {
        var s = (CustomerSession) session.getAttribute("customer");
        if (s == null) return ResponseEntity.status(401).body(Map.of("error", "Not logged in"));

        String product = (String) body.get("product");
        double amount  = body.get("amount") instanceof Number n ? n.doubleValue() : 0.0;
        int    qty     = body.get("qty")    instanceof Number n ? n.intValue()    : 1;

        // Server-side stock check
        try {
            var stock = restTemplate.getForObject(orderServiceUrl + "/api/inventory/stock", Map.class);
            if (stock != null && stock.containsKey(product)) {
                var entry = (Map<?, ?>) stock.get(product);
                int available = entry.get("qty") instanceof Number n ? n.intValue() : 0;
                if (qty > available)
                    return ResponseEntity.badRequest().body(Map.of("error", "Only " + available + " unit" + (available == 1 ? "" : "s") + " in stock"));
            }
        } catch (Exception ignored) {}

        String orderID = "ORD-" + s.customerNumber + "-" + System.currentTimeMillis();
        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\",\"amount\":%.2f,\"qty\":%d,\"status\":\"CONFIRMED\",\"createdAt\":\"%s\"}",
            orderID, s.customerID, product, amount, qty, LocalDateTime.now());
        kafka.send(orderTopic, orderID, payload);

        // Deduct inventory
        String invPayload = String.format(
            "{\"productID\":\"%s\",\"quantity\":%d,\"action\":\"ADJUST\",\"orderID\":\"%s\"}", product, -qty, orderID);
        kafka.send(inventoryTopic, product, invPayload);

        log.info("Order submitted orderID={} product={} qty={}", orderID, product, qty);
        return ResponseEntity.ok(Map.of("orderID", orderID));
    }

    // ── Cancel order ──────────────────────────────────────────────────────────
    @PostMapping("/api/cancel")
    public ResponseEntity<?> cancel(@RequestBody Map<String, String> body, HttpSession session) {
        var s = (CustomerSession) session.getAttribute("customer");
        if (s == null) return ResponseEntity.status(401).body(Map.of("error", "Not logged in"));

        String orderID = body.get("orderID");
        String reason  = body.getOrDefault("reason", "Customer requested cancellation");
        if (orderID == null) return ResponseEntity.badRequest().body(Map.of("error", "orderID required"));

        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"reason\":\"%s\",\"cancelledAt\":\"%s\"}",
            esc(orderID), esc(s.customerID), esc(reason), LocalDateTime.now());
        kafka.send(cancelledTopic, orderID, payload);

        // Restore inventory — but ONLY if stock was already deducted.
        // Stock is deducted at checkout (CONFIRMED). If the order was cancelled
        // due to payment failure it never reached CONFIRMED, so inventory is untouched.
        // Statuses where inventory IS already reserved: CONFIRMED, PAYMENT-INIT, PAYMENT-PROCESSED, PACKED
        try {
            var ordersResp = restTemplate.getForObject(
                orderServiceUrl + "/api/orders?search=" + orderID + "&size=1", Map.class);
            if (ordersResp != null) {
                var orders = (java.util.List<?>) ordersResp.get("orders");
                if (orders != null && !orders.isEmpty()) {
                    var order = (Map<?, ?>) orders.get(0);
                    String currentStatus = (String) order.get("status");
                    String product = (String) order.get("product");
                    int qty = order.get("qty") instanceof Number n ? n.intValue() : 1;

                    boolean inventoryAlreadyDeducted = currentStatus != null &&
                        java.util.Set.of("CONFIRMED","PAYMENT-INIT","PAYMENT-PROCESSED","PACKED")
                            .contains(currentStatus);

                    if (inventoryAlreadyDeducted && product != null) {
                        String invPayload = String.format(
                            "{\"productID\":\"%s\",\"quantity\":%d,\"action\":\"ADJUST\",\"orderID\":\"%s\",\"reason\":\"cancellation\"}",
                            esc(product), qty, esc(orderID));
                        kafka.send(inventoryTopic, product, invPayload);
                        log.info("Inventory restored product={} qty=+{} for cancelled orderID={}", product, qty, orderID);
                    } else {
                        log.info("Inventory NOT restored for orderID={} status={} — stock was never deducted", orderID, currentStatus);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not restore inventory for orderID={}: {}", orderID, e.getMessage());
        }

        log.info("Cancellation published orderID={}", orderID);
        return ResponseEntity.ok(Map.of("ok", true, "orderID", orderID));
    }

    // ── Stock proxy ───────────────────────────────────────────────────────────
    @GetMapping("/api/stock")
    public ResponseEntity<?> stock() {
        try {
            var resp = restTemplate.getForObject(orderServiceUrl + "/api/inventory/stock", Map.class);
            return ResponseEntity.ok(resp != null ? resp : Map.of());
        } catch (Exception e) { return ResponseEntity.ok(Map.of()); }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    private String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
