package com.employee.api;

import com.hazelblast.api.*;

@RemoteInterface
public interface EmployeeService {

    @Partitioned
    void fire(@PartitionKey String id);

    @Partitioned
    void hire(@PartitionKey String id);

    @LoadBalanced
    void printStatisticsOnOneNode();
}
