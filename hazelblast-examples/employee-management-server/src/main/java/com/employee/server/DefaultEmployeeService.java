package com.employee.server;

import com.employee.api.EmployeeService;
import com.hazelblast.server.spring.SpringPartitionListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultEmployeeService implements EmployeeService, SpringPartitionListener {

    private final static Log logger = LogFactory.getFactory().getInstance(DefaultEmployeeService.class);

    public final ConcurrentMap<Integer, Object> partitions = new ConcurrentHashMap<Integer, Object>();

    public DefaultEmployeeService() {
        logger.info("Created");
    }

    public void fire(String id) {
        logger.info("Fire called with id " + id);
    }

    public void hire(String id) {
        logger.info("Hire called with id " + id);
    }

    public void printStatisticsOnAllNodes() {
        logger.info("Statistics on all nodes, partition count " + partitions.size());
    }

    public void printStatisticsOnOneNode() {
        logger.info("Statistics on one node, partition count: " + partitions.size());
    }

    public void onPartitionAdded(int partitionId) {
        partitions.put(partitionId, this);
        logger.info("Partition added: "+partitionId+", new size is "+partitions.size());
    }

    public void onPartitionRemoved(int partitionId) {
        partitions.remove(partitionId);
        logger.info("Partition removed: "+partitionId+", new size is "+partitions.size());
    }
}
