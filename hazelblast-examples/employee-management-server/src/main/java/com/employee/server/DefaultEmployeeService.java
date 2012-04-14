package com.employee.server;

import com.employee.api.EmployeeService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultEmployeeService implements EmployeeService {

    private final static Log logger = LogFactory.getFactory().getInstance(DefaultEmployeeService.class);

    public DefaultEmployeeService(){
        logger.info("Created");
    }

    public void fire(String id) {
        logger.info("Fire called with id "+id);
    }

    public void hire(String id) {
        logger.info("Hire called with id " + id);
    }

    public void printStatisticsOnAllNodes() {
        logger.info("Statistics on all nodes");
    }

    public void printStatisticsOnOneNode() {
        logger.info("Statistics on one node");
    }
}
