package com.hazelblast.server.springslice;

import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceConfig;
import com.hazelblast.server.SliceLifecycleListener;
import com.hazelblast.server.SlicePartitionListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.partition.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;


/**
 * A Spring implementation of the {@link Slice}.
 *
 * @author Peter Veentjer.
 */
public class SpringSlice implements Slice {
    private static Log log = LogFactory.getLog(SpringSlice.class);

    private final ConfigurableApplicationContext applicationContext;
    private ExposedBeans exposedBeans;
    private final SliceConfig sliceConfig;
    private HazelcastInstance hazelcastInstance;

    public SpringSlice() {
        this(new SliceConfig(Slice.DEFAULT_NAME));
    }

    /**
     * Creates a SpringSlice which uses the {@link com.hazelcast.core.Hazelcast#getDefaultInstance()} as a
     * default {@link HazelcastInstance} if one is not provided by the application context.
     *
     * @param sliceConfig the configuration for the slice.
     * @throws NullPointerException if sliceConfig is null.
     * @throws org.springframework.beans.BeansException
     *                              if context creation failed.
     */
    public SpringSlice(SliceConfig sliceConfig) {
        this(sliceConfig, null);
    }

    /**
     * Creates a SpringSlice.
     *
     * @param sliceConfig              the configuration for the slice.
     * @param defaultHazelcastInstance the default HazelcastInstance used if none is found in the applicationcontext.
     * @throws NullPointerException if sliceConfig is null.
     * @throws org.springframework.beans.BeansException
     *                              if context creation failed
     */
    public SpringSlice(SliceConfig sliceConfig, HazelcastInstance defaultHazelcastInstance) {
        this(sliceConfig, new ClassPathXmlApplicationContext("slice.xml"), defaultHazelcastInstance);
    }

    /**
     * Creates a SpringSlice.
     *
     * @param sliceConfig              the configuration for the slice.
     * @param applicationContext       the applicationcontext to use.
     * @param defaultHazelcastInstance the default HazelcastInstance used if none is found in the applicationcontext.
     * @throws NullPointerException if sliceConfig,applicationContext is null.
     * @throws org.springframework.beans.BeansException
     *                              if context creation failed
     */
    public SpringSlice(SliceConfig sliceConfig, ConfigurableApplicationContext applicationContext, HazelcastInstance defaultHazelcastInstance) {
        this.sliceConfig = notNull("sliceConfig", sliceConfig);
        this.applicationContext = notNull("applicationContext", applicationContext);
        this.exposedBeans = applicationContext.getBean("exposedBeans", ExposedBeans.class);
        this.hazelcastInstance = findHazelcastInstance(applicationContext, defaultHazelcastInstance);
    }

    private static HazelcastInstance findHazelcastInstance(ApplicationContext appContext, HazelcastInstance defaultHazelcastInstance) {
        try {
            return appContext.getBean("hazelcastInstance", HazelcastInstance.class);
        } catch (NoSuchBeanDefinitionException e) {
            if (defaultHazelcastInstance == null) {
                return Hazelcast.getDefaultInstance();
            } else {
                return defaultHazelcastInstance;
            }
        }
    }

    /**
     * Returns the SliceConfig.
     *
     * @return the SliceConfig.
     */
    public SliceConfig getSliceConfig() {
        return sliceConfig;
    }

    /**
     * Returns the {@link ConfigurableApplicationContext} used by this SpringSlice.
     *
     * @return {@link ConfigurableApplicationContext}.
     */
    public ConfigurableApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public String getName() {
        return sliceConfig.name;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public Object getService(String name) {
        notNull("name", name);

        if (name.isEmpty()) {
            throw new IllegalArgumentException("name can't be empty");
        }

        name = name.substring(0, 1).toLowerCase() + name.substring(1);
        Object service = exposedBeans.getBean(name);
        if (service == null) {
            throw new IllegalArgumentException(format("Service with name '%s' is not found", name));
        }

        return service;
    }

    public void onStart() {
        applicationContext.start();


        for (String id : applicationContext.getBeanNamesForType(HazelcastInstanceAware.class, false, true)) {
            HazelcastInstanceAware l = (HazelcastInstanceAware) applicationContext.getBean(id);
            try {
                l.setHazelcastInstance(hazelcastInstance);
            } catch (Exception e) {
                log.warn(format("failed to call setHazelcastInstance on bean [%s] of class [%s]", id, l.getClass()), e);
            }
        }

        for (String id : applicationContext.getBeanNamesForType(SliceLifecycleListener.class, false, true)) {
            SliceLifecycleListener l = (SliceLifecycleListener) applicationContext.getBean(id);
            try {
                l.onStart();
            } catch (Exception e) {
                log.warn(format("failed to call onStart on bean [%s] of class [%s]", id, l.getClass()), e);
            }
        }
    }

    public void onStop() {
        try {
            for (String id : applicationContext.getBeanNamesForType(SliceLifecycleListener.class, false, true)) {
                SliceLifecycleListener l = (SliceLifecycleListener) applicationContext.getBean(id);
                try {
                    l.onStop();
                } catch (Exception e) {
                    log.warn(format("failed to call onStop on bean [%s] of class [%s]", id, l.getClass()), e);
                }
            }
        } finally {
            applicationContext.stop();
        }
    }

    public void onPartitionAdded(Partition partition) {
        for (String id : applicationContext.getBeanNamesForType(SlicePartitionListener.class, false, true)) {
            SlicePartitionListener l = (SlicePartitionListener) applicationContext.getBean(id);
            try {
                l.onPartitionAdded(partition);
            } catch (Exception e) {
                log.warn(format("failed to call onPartitionAdded on bean [%s] of class [%s]", id, l.getClass()), e);
            }
        }
    }

    public void onPartitionRemoved(Partition partition) {
        for (String id : applicationContext.getBeanNamesForType(SlicePartitionListener.class, false, true)) {
            SlicePartitionListener l = (SlicePartitionListener) applicationContext.getBean(id);
            try {
                l.onPartitionRemoved(partition);
            } catch (Exception e) {
                log.warn(format("failed to call onPartitionRemoved on bean [%s] of class [%s]", id, l.getClass()), e);
            }
        }
    }
}
