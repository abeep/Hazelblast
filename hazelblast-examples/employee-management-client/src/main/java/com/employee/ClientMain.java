package com.employee;

import com.employee.api.EmployeeService;
import com.hazelcast.hazelblast.client.ProxyProvider;

public class ClientMain {

    public static void main(String[] args) {
        ProxyProvider proxyProvider = new ProxyProvider();
        EmployeeService service = proxyProvider.getProxy(EmployeeService.class);

        System.out.println("Partitioned call");
        service.fire("1");
        System.out.println("Finished");

        System.out.println("Partitioned call");
        service.hire("2");
        System.out.println("Finished");

        System.out.println("ForkJoin call");
        service.printStatisticsOnAllNodes();
        System.out.println("Finished");

        System.out.println("LoadBalanced call");
        service.printStatisticsOnOneNode();
        System.out.println("Finished");
    }
}
