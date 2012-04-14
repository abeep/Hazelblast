package com.employee.api;

import com.hazelcast.hazelblast.api.ForkJoin;
import com.hazelcast.hazelblast.api.LoadBalanced;
import com.hazelcast.hazelblast.api.Partitioned;
import com.hazelcast.hazelblast.api.RoutingId;

public interface EmployeeService {

    @Partitioned
    void fire(@RoutingId String id);

    @Partitioned
    void hire(@RoutingId String id);

    @ForkJoin
    void printStatisticsOnAllNodes();

    @LoadBalanced
    void printStatisticsOnOneNode();
}
