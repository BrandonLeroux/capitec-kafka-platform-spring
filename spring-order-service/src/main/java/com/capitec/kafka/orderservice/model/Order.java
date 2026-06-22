package com.capitec.kafka.orderservice.model;

public class Order {
    public String orderID;
    public String customerID;
    public String product;
    public double amount;
    public int    qty;
    public String status;
    public String cancellationReason;
    public String receivedAt;
    public String updatedAt;
}
