package com.example.kafkastreams.model;

public class Order {

    private String orderId;
    private String customerId;
    private double amount;
    private String description;

    public Order() {}

    public Order(String orderId, String customerId, double amount, String description) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.description = description;
    }

    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    @Override
    public String toString() {
        return "Order{orderId='" + orderId + "', customerId='" + customerId
                + "', amount=" + amount + ", description='" + description + "'}";
    }
}
