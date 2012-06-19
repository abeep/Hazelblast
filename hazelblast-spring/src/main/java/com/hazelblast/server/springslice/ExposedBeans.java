package com.hazelblast.server.springslice;

import java.util.HashMap;
import java.util.Map;

import static com.hazelblast.utils.Arguments.notNull;

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
        return beans.get(notNull("beanName",beanName));
    }

    /**
     * Sets the beans that can be exposed.
     *
     * @param beans the beans to expose.
     */
    public void setBeans(Map<String, Object> beans) {
        this.beans = notNull("beans",beans);
    }
}
