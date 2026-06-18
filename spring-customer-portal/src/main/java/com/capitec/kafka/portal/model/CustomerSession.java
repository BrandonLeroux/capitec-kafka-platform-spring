package com.capitec.kafka.portal.model;

public class CustomerSession {
    public String customerID;
    public long   customerNumber;
    public String firstName;
    public String cell;

    public CustomerSession() {}

    public CustomerSession(String customerID, long customerNumber, String firstName, String cell) {
        this.customerID     = customerID;
        this.customerNumber = customerNumber;
        this.firstName      = firstName;
        this.cell           = cell;
    }
}
