package com.employee;

import com.employee.api.EmployeeService;
import com.hazelblast.client.ProxyProvider;

public class ClientMain {

    public static void main(String[] args) {
        ProxyProvider proxyProvider = new ProxyProvider();
        EmployeeService service = proxyProvider.getProxy(EmployeeService.class);

        System.out.println("Partitioned call");
        for (int k = 0; k < 100; k++) {
            service.fire(""+k);
            System.out.println("Finished");
        }


        System.out.println("ForkJoin call");
        service.printStatisticsOnAllNodes();
        System.out.println("Finished");

        System.out.println("LoadBalanced call");
        service.printStatisticsOnOneNode();
        System.out.println("Finished");
    }
}
