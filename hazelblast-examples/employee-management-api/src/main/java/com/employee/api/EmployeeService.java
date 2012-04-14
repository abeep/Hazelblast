package com.employee.api;

import com.hazelblast.api.ForkJoin;
import com.hazelblast.api.LoadBalanced;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RoutingId;

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
