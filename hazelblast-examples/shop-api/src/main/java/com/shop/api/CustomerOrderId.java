package com.shop.api;

import com.hazelcast.core.PartitionAware;

import java.io.Serializable;

import static com.hazelblast.utils.Arguments.notNull;

public class CustomerOrderId implements Serializable, PartitionAware {
    private final String customerId;
    private final String orderId;

    public CustomerOrderId(String customerId, String orderId) {
        this.customerId = notNull("customerId",customerId);
        this.orderId = notNull("orderId",orderId);
    }

    public Object getPartitionKey() {
        return customerId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getOrderId() {
        return orderId;
    }

    @Override
    public String toString() {
        return "CustomerOrderId{" +
                "customerId='" + customerId + '\'' +
                ", orderId='" + orderId + '\'' +
                '}';
    }
}
