package com.shop.server;

import com.shop.api.Customer;
import com.shop.api.CustomerService;
import com.hazelcast.core.Hazelcast;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ConcurrentMap;

public class DefaultCustomerService implements CustomerService {

    private final static Log logger = LogFactory.getFactory().getInstance(DefaultCustomerService.class);

    private final ConcurrentMap<String, Customer> customers = Hazelcast.getMap("customers");

    public Customer get(String id) {
        return customers.get(id);
    }

    public String create(String id, String name) {
        if(customers.containsKey(id)){
            throw new IllegalArgumentException();
        }
        Customer customer = new Customer(id);
        customer.setName(name);
        customers.put(id, customer);

        logger.info("Hire called with id " + id);
        return customer.getId();
    }

    public void printStatisticsOnOneNode() {
        logger.info("Statistics on one node, partition count: " + customers.size());
    }
}
