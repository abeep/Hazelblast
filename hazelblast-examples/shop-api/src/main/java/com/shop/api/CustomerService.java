package com.shop.api;

import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;

@DistributedService
public interface CustomerService {

    @Partitioned
    String create(@PartitionKey String id, String name);

    @Partitioned
    Customer get(@PartitionKey String id);
}
