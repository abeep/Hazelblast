package com.shop.server;

import com.hazelcast.core.Hazelcast;
import com.shop.api.CustomerOrderId;
import com.shop.api.Order;
import com.shop.api.OrderLine;
import com.shop.api.OrderService;

import java.util.Map;
import java.util.UUID;

import static com.hazelblast.utils.Arguments.notNull;

public class DefaultOrderService implements OrderService {

    private final Map<CustomerOrderId, Order> orders = Hazelcast.getMap("orders");

    public Order get(CustomerOrderId customerOrderId) {
        notNull("customerOrderId",customerOrderId);

        return orders.get(customerOrderId);
    }

    public CustomerOrderId createOrder(String customerId) {
        notNull("customerId",customerId);

        String orderId = UUID.randomUUID().toString();
        Order order = new Order(orderId, customerId);

        CustomerOrderId customerOrderId = new CustomerOrderId(customerId, orderId);
        orders.put(customerOrderId, order);
        return customerOrderId;
    }

    public void addOrderLine(CustomerOrderId customerOrderId, String articleId, int quantity) {
        notNull("customerOrderId",customerOrderId);
        notNull("articleId",articleId);

        Order order = orders.get(customerOrderId);
        if (order == null) {
            throw new IllegalArgumentException();
        }

        OrderLine orderLine = new OrderLine(articleId,quantity);
        order.getLines().add(orderLine);
        orders.put(customerOrderId, order);
    }
}
