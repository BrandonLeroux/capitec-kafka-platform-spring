package com.capitec.kafka.orderservice.consumer;

import com.capitec.kafka.orderservice.model.Customer;
import com.capitec.kafka.orderservice.model.Order;
import com.capitec.kafka.orderservice.producer.PaymentProducer;
import com.capitec.kafka.orderservice.repository.CustomerRepository;
import com.capitec.kafka.orderservice.repository.OrderRepository;
import com.capitec.kafka.orderservice.service.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);

    private final OrderRepository    orderRepo;
    private final CustomerRepository customerRepo;
    private final PaymentProducer    paymentProducer;
    private final JsonParser         parser;

    public OrderEventConsumer(OrderRepository orderRepo, CustomerRepository customerRepo,
                              PaymentProducer paymentProducer, JsonParser parser) {
        this.orderRepo       = orderRepo;
        this.customerRepo    = customerRepo;
        this.paymentProducer = paymentProducer;
        this.parser          = parser;
    }

    @KafkaListener(topics = "${app.topics.order}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrder(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            Order order = parser.parseOrder(record.value());
            if (order == null) { log.warn("Could not parse order key={}", record.key()); return; }
            orderRepo.upsert(order);
            log.info("Order upserted orderID={} status={}", order.orderID, order.status);
            if ("CONFIRMED".equalsIgnoreCase(order.status)) {
                paymentProducer.sendPaymentInstruction(order);
                orderRepo.updateStatus(order.orderID, "PAYMENT-INIT");
                log.info("Payment instruction sent orderID={}", order.orderID);
            }
        } catch (Exception e) {
            log.error("Failed to process order record key={}", record.key(), e);
        } finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = "${app.topics.customer}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeCustomer(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            Customer c = parser.parseCustomer(record.value());
            if (c == null) { log.warn("Could not parse customer key={}", record.key()); return; }
            customerRepo.upsert(c);
            log.info("Customer upserted id={} number={}", c.customerID, c.customerNumber);
        } catch (Exception e) {
            log.error("Failed to process customer record key={}", record.key(), e);
        } finally {
            ack.acknowledge();
        }
    }

    @KafkaListener(topics = "${app.topics.cancelled}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeCancellation(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String orderID = parser.getString(record.value(), "orderID");
            String reason  = parser.getString(record.value(), "reason");
            if (orderID == null) { log.warn("Cancellation missing orderID key={}", record.key()); return; }
            orderRepo.updateCancelled(orderID, reason != null ? reason : "No reason provided");
            log.info("Order cancelled orderID={} reason={}", orderID, reason);
        } catch (Exception e) {
            log.error("Failed to process cancellation key={}", record.key(), e);
        } finally {
            ack.acknowledge();
        }
    }
}
