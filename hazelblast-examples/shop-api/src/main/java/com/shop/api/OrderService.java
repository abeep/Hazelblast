package com.shop.api;

import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.annotations.RemoteInterface;

@RemoteInterface
public interface OrderService {

    @Partitioned
    Order get(@PartitionKey CustomerOrderId customerOrderId);

    @Partitioned
    CustomerOrderId createOrder(@PartitionKey String customerId);

    @Partitioned
    void addOrderLine(@PartitionKey CustomerOrderId customerOrderId, String articleId, int quantity);
}
