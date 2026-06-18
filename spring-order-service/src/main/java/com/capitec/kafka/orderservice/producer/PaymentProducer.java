package com.capitec.kafka.orderservice.producer;

import com.capitec.kafka.orderservice.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentProducer {

    private static final Logger log = LoggerFactory.getLogger(PaymentProducer.class);
    private final KafkaTemplate<String, String> kafka;
    private final String paymentTopic;

    public PaymentProducer(KafkaTemplate<String, String> kafka,
                           @Value("${app.topics.payment}") String paymentTopic) {
        this.kafka        = kafka;
        this.paymentTopic = paymentTopic;
    }

    public void sendPaymentInstruction(Order order) {
        String payload = String.format(
            "{\"orderID\":\"%s\",\"customerID\":\"%s\",\"product\":\"%s\",\"amount\":%.2f,\"instruction\":\"INITIATE_PAYMENT\"}",
            order.orderID, order.customerID, order.product, order.amount);
        kafka.send(paymentTopic, order.orderID, payload)
             .whenComplete((r, ex) -> {
                 if (ex != null) log.error("Failed to send payment instruction orderID={}", order.orderID, ex);
                 else log.info("Payment instruction sent orderID={} offset={}", order.orderID, r.getRecordMetadata().offset());
             });
    }
}
