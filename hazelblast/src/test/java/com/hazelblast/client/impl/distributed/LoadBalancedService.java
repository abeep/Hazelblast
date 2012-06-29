package com.hazelblast.client.impl.distributed;

import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;

@DistributedService
public interface LoadBalancedService {
    @LoadBalanced
    public void test();
}
