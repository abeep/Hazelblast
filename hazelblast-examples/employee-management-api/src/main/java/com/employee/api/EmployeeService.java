package com.employee.api;

import com.hazelblast.api.*;
import com.hazelblast.api.reducers.VoidReducer;

@RemoteInterface
public interface EmployeeService {

    @Partitioned
    void fire(@RoutingId String id);

    @Partitioned
    void hire(@RoutingId String id);

    @ForkJoin (reducer = VoidReducer.class)
    void printStatisticsOnAllNodes();

    @LoadBalanced
    void printStatisticsOnOneNode();
}
