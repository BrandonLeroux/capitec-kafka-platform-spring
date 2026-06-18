package com.capitec.kafka.inventory.model;

public class InventoryItem {
    public String sku;
    public String productID;
    public String name;
    public String category;
    public int    quantity;
    public int    reorderLevel;
    public double unitPrice;
    public String updatedAt;
}
