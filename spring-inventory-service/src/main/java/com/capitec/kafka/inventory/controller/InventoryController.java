package com.capitec.kafka.inventory.controller;

import com.capitec.kafka.inventory.model.InventoryItem;
import com.capitec.kafka.inventory.repository.InventoryRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class InventoryController {

    private final InventoryRepository repo;

    public InventoryController(InventoryRepository repo) { this.repo = repo; }

    @GetMapping("/api/inventory")
    public Map<String, Object> getInventory(
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String category,
            @RequestParam(defaultValue = "1")   int page,
            @RequestParam(defaultValue = "200") int size) {
        int offset = (page - 1) * size;
        List<InventoryItem> items = repo.findAll(search, category, size, offset);
        return Map.of("total", repo.count(search, category), "lowStock", repo.countLowStock(), "items", items);
    }

    @GetMapping("/api/inventory/stock")
    public Map<String, Object> stockMap() {
        return repo.stockMap();
    }

    @GetMapping("/api/inventory/sku/{sku}")
    public ResponseEntity<?> getBySku(@PathVariable String sku) {
        InventoryItem item = repo.findBySku(sku);
        return item != null ? ResponseEntity.ok(item) : ResponseEntity.notFound().build();
    }

    @PostMapping("/api/inventory/seed")
    public ResponseEntity<?> seed(@RequestBody Map<String, Object> body) {
        InventoryItem item = new InventoryItem();
        item.sku          = (String) body.get("sku");
        item.productID    = (String) body.get("productID");
        item.name         = (String) body.get("name");
        item.category     = (String) body.get("category");
        item.quantity     = body.get("quantity")     != null ? ((Number) body.get("quantity")).intValue()     : 0;
        item.reorderLevel = body.get("reorderLevel") != null ? ((Number) body.get("reorderLevel")).intValue() : 10;
        item.unitPrice    = body.get("unitPrice")    != null ? ((Number) body.get("unitPrice")).doubleValue() : 0.0;
        if (item.sku == null) return ResponseEntity.badRequest().body(Map.of("error", "sku required"));
        repo.upsert(item);
        return ResponseEntity.ok(Map.of("sku", item.sku));
    }
}
