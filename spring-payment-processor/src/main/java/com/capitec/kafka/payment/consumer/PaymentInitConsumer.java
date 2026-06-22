package com.capitec.kafka.payment.consumer;

import com.capitec.kafka.payment.service.FulfilmentService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PaymentInitConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentInitConsumer.class);
    private final FulfilmentService fulfilment;

    public PaymentInitConsumer(FulfilmentService fulfilment) {
        this.fulfilment = fulfilment;
    }

    @KafkaListener(topics = "${app.topics.payment-init}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String json       = record.value();
            String orderID    = extract(json, "orderID");
            String customerID = extract(json, "customerID");
            String product    = extract(json, "product");
            double amount     = extractDouble(json, "amount");

            if (orderID == null) { log.warn("Missing orderID key={}", record.key()); return; }

            int qty = extractInt(json, "qty");
            if (qty <= 0) qty = 1;

            log.info("Payment triggered orderID={} product={} qty={}", orderID, product, qty);
            fulfilment.process(orderID, customerID != null ? customerID : "",
                               product != null ? product : "", amount, qty);
        } finally {
            ack.acknowledge();
        }
    }

    private String extract(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key); if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length()); if (colon < 0) return null;
        int start = json.indexOf('"', colon + 1); if (start < 0) return null;
        int end = json.indexOf('"', start + 1); if (end < 0) return null;
        return json.substring(start + 1, end);
    }

    private int extractInt(String json, String field) {
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

    private double extractDouble(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key); if (idx < 0) return 0.0;
        int colon = json.indexOf(':', idx + key.length()); if (colon < 0) return 0.0;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        int end = start;
        while (end < json.length() && ",}".indexOf(json.charAt(end)) < 0) end++;
        try { return Double.parseDouble(json.substring(start, end).trim().replace(',', '.')); } catch (Exception e) { return 0.0; }
    }
}
