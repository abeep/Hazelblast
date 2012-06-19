package com.shop.api;

import com.hazelblast.api.*;

@RemoteInterface
public interface CustomerService {

    @Partitioned
    String create(@PartitionKey String id, String name);

    @Partitioned
    Customer get(@PartitionKey String id);
}
