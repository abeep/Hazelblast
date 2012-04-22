package com.hazelblast.server.spring;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ExposedServices is a registration point for all spring beans that should be made available
 * to be called from the outside world.
 *
 * @author Peter Veentjer.
 */
public class ExposedServices {

    private Map<String, Object> exposedServices = new HashMap<String, Object>();

    public Object getService(String serviceName) {
        if (serviceName == null) {
            throw new NullPointerException("serviceName can't be null");
        }

        return exposedServices.get(serviceName);
    }

    public void setExposedServices(Map<String,Object> exposedServices) {
        if(exposedServices == null){
            throw new NullPointerException("exposedServices can't be null");
        }

        this.exposedServices = exposedServices;
    }
}
