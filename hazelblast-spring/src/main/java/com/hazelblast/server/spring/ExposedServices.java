package com.hazelblast.server.spring;

import java.util.LinkedList;
import java.util.List;

//todo: the list is ugly.
public class ExposedServices {

    private List<String> exposedServices = new LinkedList<String>();

    public List getExposedServices() {
        return exposedServices;
    }

    public boolean isExposed(String serviceName){
        for(String s: exposedServices){
            if(s.equals(serviceName)){
                return true;
            }
        }

        return false;
    }

    public void setExposedServices(List<String> exposedServices) {
        this.exposedServices = exposedServices;
    }
}
