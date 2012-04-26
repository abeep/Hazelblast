package com.employee.api;

import com.hazelblast.api.*;
import com.hazelblast.api.reducers.VoidReducer;

@RemoteInterface
public interface EmployeeService {

    @Partitioned
    void fire(@PartitionKey String id);

    @Partitioned
    void hire(@PartitionKey String id);

    @ForkJoin (reducer = VoidReducer.class)
    void printStatisticsOnAllNodes();

    @LoadBalanced
    void printStatisticsOnOneNode();
}
