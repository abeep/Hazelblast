package com.employee.server;

import com.employee.api.EmployeeService;
import com.hazelblast.server.spring.SpringPartitionListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.MultiMap;
import com.hazelcast.partition.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultEmployeeService implements EmployeeService, SpringPartitionListener {

    private final static Log logger = LogFactory.getFactory().getInstance(DefaultEmployeeService.class);

    private final ConcurrentMap<Integer, Object> partitions = new ConcurrentHashMap<Integer, Object>();

    private final MultiMap<Integer, String> idsPerPartitionMap;

    private final ConcurrentMap<String, Object> localEmployees = new ConcurrentHashMap<String, Object>();

    public DefaultEmployeeService(MultiMap<Integer,String> idsPerPartitionMap) {

        logger.info("Created");

        this.idsPerPartitionMap = idsPerPartitionMap;
    }

    public void fire(String id) {
        Partition partition = Hazelcast.getPartitionService().getPartition(id);
        idsPerPartitionMap.put(partition.getPartitionId(), id);
        localEmployees.put(id,this);

        logger.info("Fire called with id " + id);
    }

    public void hire(String id) {
        Partition partition = Hazelcast.getPartitionService().getPartition(id);
        idsPerPartitionMap.put(partition.getPartitionId(), id);
        localEmployees.put(id,this);

        logger.info("Hire called with id " + id);
    }

    public void printStatisticsOnAllNodes() {
        logger.info("Statistics on all nodes, partition count " + partitions.size());
    }

    public void printStatisticsOnOneNode() {
        logger.info("Statistics on one node, partition count: " + partitions.size());
    }

    public void onPartitionAdded(int partitionId) {
        Collection<String> ids = idsPerPartitionMap.get(partitionId);
        if (ids != null) {
            for (String id : ids) {
                localEmployees.put(id, this);
            }
        }

        partitions.put(partitionId, this);
        logger.debug("Partition added: " + partitionId + ", partitioncount:" + partitions.size()+" localemployees: "+localEmployees.size());
    }

    public void onPartitionRemoved(int partitionId) {
        Collection<String> ids = idsPerPartitionMap.get(partitionId);
        if (ids != null) {
            for (String id : ids) {
                localEmployees.remove(id);
            }
        }

        partitions.remove(partitionId);
        logger.debug("Partition removed: " + partitionId + ", partitioncount: " + partitions.size()+" localemployees: "+localEmployees.size());
    }

    public void init() {
        logger.info("Initializing");
    }

    public void destroy() {
        logger.info("Destroying");
    }
}
