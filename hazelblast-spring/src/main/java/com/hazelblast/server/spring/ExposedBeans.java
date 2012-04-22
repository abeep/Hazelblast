package com.hazelblast.server.spring;

import java.util.HashMap;
import java.util.Map;

/**
 * The ExposedBeans is a registration point for all spring beans that should be made available
 * to be called from the outside world.
 *
 * @author Peter Veentjer.
 */
public class ExposedBeans {

    private Map<String, Object> beans = new HashMap<String, Object>();

    /**
     * Gets the service with the given name.
     *
     * @param beanName the name of the service to get.
     * @return the found bean or null if not found.
     */
    public Object getBean(String beanName) {
        if (beanName == null) {
            throw new NullPointerException("beanName can't be null");
        }

        return beans.get(beanName);
    }

    /**
     * Sets the beans that can be exposed.
     *
     * @param beans the beans to expose.
     */
    public void setBeans(Map<String, Object> beans) {
        if(beans == null){
            throw new NullPointerException("beans can't be null");
        }

        this.beans = beans;
    }
}
