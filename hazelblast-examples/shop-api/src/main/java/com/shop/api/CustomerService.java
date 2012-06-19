package com.shop.api;

import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.annotations.RemoteInterface;

@RemoteInterface
public interface CustomerService {

    @Partitioned
    String create(@PartitionKey String id, String name);

    @Partitioned
    Customer get(@PartitionKey String id);
}
