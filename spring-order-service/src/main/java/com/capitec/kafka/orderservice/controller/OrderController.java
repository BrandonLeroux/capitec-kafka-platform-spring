package com.capitec.kafka.orderservice.controller;

import com.capitec.kafka.orderservice.model.Customer;
import com.capitec.kafka.orderservice.model.Order;
import com.capitec.kafka.orderservice.repository.CustomerRepository;
import com.capitec.kafka.orderservice.repository.OrderRepository;
import com.capitec.kafka.orderservice.service.JsonParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class OrderController {

    private final OrderRepository    orderRepo;
    private final CustomerRepository customerRepo;
    private final JsonParser         parser;
    private final RestTemplate       restTemplate = new RestTemplate();

    @Value("${app.inventory-service.url:http://spring-inventory-service:8083}")
    private String inventoryServiceUrl;

    public OrderController(OrderRepository orderRepo, CustomerRepository customerRepo, JsonParser parser) {
        this.orderRepo    = orderRepo;
        this.customerRepo = customerRepo;
        this.parser       = parser;
    }

    // ── Orders ────────────────────────────────────────────────────────────────
    @GetMapping("/api/orders")
    public Map<String, Object> getOrders(
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String customerID,
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "20") int size) {

        int offset = (page - 1) * size;
        List<Order> orders = orderRepo.findAll(search, status, customerID, size, offset);
        int total = orderRepo.count(search, status, customerID);

        Map<String, Long> stats = new HashMap<>();
        for (String s : new String[]{"CONFIRMED","PAYMENT-INIT","PAYMENT-PROCESSED","PACKED","OUT-FOR-DELIVERY","DELIVERED","CANCELLED"})
            stats.put(s, orderRepo.countByStatus(s));

        return Map.of("total", total, "page", page, "pageSize", size, "stats", stats, "orders", orders);
    }

    @PostMapping("/api/order/seed")
    public ResponseEntity<?> seedOrder(@RequestBody Map<String, Object> body) {
        Order o = new Order();
        o.orderID    = (String) body.get("orderID");
        o.customerID = (String) body.get("customerID");
        o.product    = (String) body.get("product");
        o.amount     = body.get("amount") != null ? ((Number) body.get("amount")).doubleValue() : 0.0;
        o.status     = (String) body.get("status");
        if (o.orderID == null) return ResponseEntity.badRequest().body(Map.of("error", "orderID required"));
        orderRepo.seedUpsert(o);
        return ResponseEntity.ok(Map.of("orderID", o.orderID));
    }

    // ── Customers ─────────────────────────────────────────────────────────────
    @GetMapping("/api/customers")
    public Map<String, Object> getCustomers(
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "1")  int page,
            @RequestParam(defaultValue = "20") int size) {
        int offset = (page - 1) * size;
        return Map.of("total", customerRepo.count(search), "customers", customerRepo.findAll(search, size, offset));
    }

    @GetMapping("/api/customer/by-cell")
    public ResponseEntity<?> getByCell(@RequestParam String cell) {
        Customer c = customerRepo.findByIdentifier(cell);
        return c != null ? ResponseEntity.ok(c) : ResponseEntity.notFound().build();
    }

    @GetMapping("/api/customer/by-identifier")
    public ResponseEntity<?> getByIdentifier(@RequestParam String q) {
        Customer c = customerRepo.findByIdentifier(q.trim());
        return c != null ? ResponseEntity.ok(c) : ResponseEntity.notFound().build();
    }

    @GetMapping("/api/customer/max-number")
    public Map<String, Long> maxCustomerNumber() {
        return Map.of("maxCustomerNumber", customerRepo.maxCustomerNumber());
    }

    @GetMapping("/api/customer/{id}")
    public ResponseEntity<?> getCustomer(@PathVariable String id) {
        Customer c = customerRepo.findById(id);
        return c != null ? ResponseEntity.ok(c) : ResponseEntity.notFound().build();
    }

    @PostMapping("/api/customer/register")
    public ResponseEntity<?> registerCustomer(@RequestBody Map<String, Object> body) {
        Customer c = new Customer();
        c.customerID     = (String) body.get("customerID");
        c.firstName      = (String) body.get("firstName");
        c.lastName       = (String) body.get("lastName");
        c.idNumber       = (String) body.get("idNumber");
        c.email          = (String) body.get("email");
        c.cell           = (String) body.get("cell");
        c.passwordHash   = (String) body.get("passwordHash");
        c.customerNumber = body.get("customerNumber") != null ? ((Number) body.get("customerNumber")).longValue() : 0L;
        if (c.customerID == null) return ResponseEntity.badRequest().body(Map.of("error", "customerID required"));
        customerRepo.upsert(c);
        return ResponseEntity.ok(Map.of("customerID", c.customerID));
    }

    // ── Inventory proxy ───────────────────────────────────────────────────────
    @GetMapping("/api/inventory")
    public ResponseEntity<?> inventoryProxy(@RequestParam Map<String, String> params) {
        try {
            String query = params.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .reduce((a, b) -> a + "&" + b).orElse("");
            String url = inventoryServiceUrl + "/api/inventory" + (query.isEmpty() ? "" : "?" + query);
            var result = restTemplate.getForObject(url, Map.class);
            return ResponseEntity.ok(result != null ? result : Map.of("total", 0, "items", new Object[0]));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of("total", 0, "lowStock", 0, "items", new Object[0]));
        }
    }

    @GetMapping("/api/inventory/stock")
    public ResponseEntity<?> inventoryStockProxy() {
        try {
            var result = restTemplate.getForObject(inventoryServiceUrl + "/api/inventory/stock", Map.class);
            return ResponseEntity.ok(result != null ? result : Map.of());
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of());
        }
    }

    // ── Dashboard UI ──────────────────────────────────────────────────────────
    @GetMapping("/")
    public ResponseEntity<String> dashboard() {
        return ResponseEntity.ok()
            .header("Content-Type", "text/html; charset=UTF-8")
            .body(DashboardHtml.build());
    }
}
