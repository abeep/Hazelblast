package com.shop;

import com.shop.api.*;
import com.hazelblast.client.DefaultProxyProvider;
import com.hazelblast.client.ProxyProvider;

import java.util.UUID;

public class ClientMain {

    public static void main(String[] args) {
        ProxyProvider proxyProvider = new DefaultProxyProvider();
        CustomerService customerService = proxyProvider.getProxy(CustomerService.class);
        OrderService orderService = proxyProvider.getProxy(OrderService.class);

        String customerId = UUID.randomUUID().toString();
        customerService.create(customerId,"peter");
        Customer customer = customerService.get(customerId);

        CustomerOrderId orderId = orderService.createOrder(customerId);
        for(int k=0;k<10;k++){
            orderService.addOrderLine(orderId,"article-"+k,1);
        }

        Order order = orderService.get(orderId);
    }
}
