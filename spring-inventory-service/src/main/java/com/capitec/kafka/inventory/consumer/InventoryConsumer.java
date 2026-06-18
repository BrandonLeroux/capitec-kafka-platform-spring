package com.capitec.kafka.inventory.consumer;

import com.capitec.kafka.inventory.model.InventoryItem;
import com.capitec.kafka.inventory.repository.InventoryRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);
    private final InventoryRepository repo;

    public InventoryConsumer(InventoryRepository repo) { this.repo = repo; }

    @KafkaListener(topics = "${app.topics.inventory}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String json = record.value();
            InventoryItem item = new InventoryItem();
            item.sku          = get(json, "sku");
            item.productID    = get(json, "productID");
            item.name         = get(json, "name");
            item.category     = get(json, "category");
            item.quantity     = getInt(json, "quantity");
            item.reorderLevel = getInt(json, "reorderLevel");
            item.unitPrice    = getDouble(json, "unitPrice");

            if (item.sku == null && item.productID != null) {
                InventoryItem existing = repo.findByProductId(item.productID);
                if (existing != null) item.sku = existing.sku;
            }
            if (item.sku == null) { log.warn("Cannot resolve sku key={}", record.key()); return; }

            String action = get(json, "action");
            if ("ADJUST".equals(action)) {
                repo.adjust(item.sku, item.quantity);
                log.info("Inventory adjusted sku={} delta={}", item.sku, item.quantity);
            } else {
                repo.upsert(item);
                log.info("Inventory set sku={} qty={}", item.sku, item.quantity);
            }
        } catch (Exception e) {
            log.error("Failed to process inventory record key={}", record.key(), e);
        } finally {
            ack.acknowledge();
        }
    }

    private String get(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key); if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length()); if (colon < 0) return null;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (start >= json.length() || json.charAt(start) != '"') return null;
        start++; int end = start;
        while (end < json.length()) { if (json.charAt(end) == '"' && json.charAt(end-1) != '\\') break; end++; }
        return json.substring(start, end);
    }

    private int getInt(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key); if (idx < 0) return 0;
        int colon = json.indexOf(':', idx + key.length()); if (colon < 0) return 0;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (json.charAt(start) == '"') start++;
        int end = start;
        while (end < json.length() && ",}\"".indexOf(json.charAt(end)) < 0) end++;
        try { return Integer.parseInt(json.substring(start, end).trim()); } catch (Exception e) { return 0; }
    }

    private double getDouble(String json, String field) {
        try { return Double.parseDouble(String.valueOf(getInt(json, field))); } catch (Exception e) { return 0.0; }
    }
}
