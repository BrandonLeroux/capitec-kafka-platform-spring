package com.capitec.kafka.orderservice.service;

import com.capitec.kafka.orderservice.model.Customer;
import com.capitec.kafka.orderservice.model.Order;
import org.springframework.stereotype.Component;

@Component
public class JsonParser {

    public String getString(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return null;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (start >= json.length() || json.charAt(start) != '"') return null;
        start++;
        int end = start;
        while (end < json.length()) {
            if (json.charAt(end) == '"' && json.charAt(end - 1) != '\\') break;
            end++;
        }
        return json.substring(start, end).replace("\\\"", "\"");
    }

    public long getLong(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return 0L;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return 0L;
        int start = colon + 1;
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (json.charAt(start) == '"') start++;
        int end = start;
        while (end < json.length() && ",}\"".indexOf(json.charAt(end)) < 0) end++;
        try { return Long.parseLong(json.substring(start, end).trim()); } catch (Exception e) { return 0L; }
    }

    public double getDouble(String json, String field) {
        String val = getString(json, field);
        if (val == null) {
            String key = "\"" + field + "\"";
            int idx = json.indexOf(key);
            if (idx < 0) return 0.0;
            int colon = json.indexOf(':', idx + key.length());
            if (colon < 0) return 0.0;
            int start = colon + 1;
            while (start < json.length() && json.charAt(start) == ' ') start++;
            int end = start;
            while (end < json.length() && ",}".indexOf(json.charAt(end)) < 0) end++;
            val = json.substring(start, end).trim();
        }
        try { return Double.parseDouble(val.replace(',', '.')); } catch (Exception e) { return 0.0; }
    }

    public Order parseOrder(String json) {
        if (json == null || json.isBlank()) return null;
        String merged = json.contains("},{") ? json.replace("},{", ",") : json;
        String orderID = getString(merged, "orderID");
        if (orderID == null) return null;
        Order o = new Order();
        o.orderID    = orderID;
        o.customerID = getString(merged, "customerID");
        o.product    = getString(merged, "product");
        o.amount     = getDouble(merged, "amount");
        o.qty        = (int) getLong(merged, "qty");
        if (o.qty <= 0) o.qty = 1;
        o.status     = getString(merged, "status");
        if (o.customerID == null) o.customerID = "UNKNOWN";
        if (o.product    == null) o.product    = "UNKNOWN";
        if (o.status     == null) o.status     = "PENDING";
        return o;
    }

    public Customer parseCustomer(String json) {
        if (json == null || json.isBlank()) return null;
        String customerID = getString(json, "customerID");
        if (customerID == null) return null;
        Customer c = new Customer();
        c.customerID     = customerID;
        c.firstName      = getString(json, "firstName");
        c.lastName       = getString(json, "lastName");
        c.idNumber       = getString(json, "idNumber");
        c.email          = getString(json, "email");
        c.cell           = getString(json, "cell");
        c.passwordHash   = getString(json, "passwordHash");
        c.customerNumber = getLong(json, "customerNumber");
        return c;
    }
}
