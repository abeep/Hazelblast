package com.shop.api;

import com.hazelblast.api.PartitionKey;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RemoteInterface;

@RemoteInterface
public interface OrderService {

    @Partitioned
    Order get(CustomerOrderId customerOrderId);

    @Partitioned
    CustomerOrderId createOrder(@PartitionKey String customerId);

    @Partitioned
    void addOrderLine(CustomerOrderId customerOrderId, String articleId, int quantity);
}
