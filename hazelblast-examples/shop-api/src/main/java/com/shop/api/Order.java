package com.shop.api;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import static com.hazelblast.utils.Arguments.notNull;

public class Order implements Serializable{

    private final String orderId;
    private final String customerId;
    private final List<OrderLine> lines = new LinkedList<OrderLine>();

    public Order(String orderId, String customerId){
       this.orderId = notNull("orderId",orderId);
       this.customerId = notNull("customerId",customerId);
    }

    public List<OrderLine> getLines() {
        return lines;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", lines=" + lines +
                '}';
    }
}
