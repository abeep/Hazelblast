package com.shop.api;

import com.hazelblast.api.PartitionKey;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RemoteInterface;

@RemoteInterface
public interface OrderService {

    @Partitioned
    Order get(@PartitionKey CustomerOrderId customerOrderId);

    @Partitioned
    CustomerOrderId createOrder(@PartitionKey String customerId);

    @Partitioned
    void addOrderLine(@PartitionKey CustomerOrderId customerOrderId, String articleId, int quantity);
}
