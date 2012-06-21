package com.hazelblast.server.springslice;

import com.hazelblast.server.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;

import static java.lang.String.format;

/**
 * A Spring based {@link com.hazelblast.server.SliceFactory}.
 * <p/>
 * It expects a 'slice.xml' to be available in the root of the jar.
 * <p/>
 * <h2>exposedBeans bean</h2>
 * The slice.xml should contain a bean called 'exposedBeans' where all the beans that
 * should be exposed remotely, should be registered.
 * <pre>
 * {@code
 *  <bean id="exposedBeans" class="com.hazelblast.server.springslice.ExposedBeans">
 *      <property name="exposedBeans">
 *          <map>
 *              <entry key="employeeService" value-ref="employeeService"/>
 *          </map>
 *      </property>
 *  </bean>
 * </pre>
 * The reason for this functionality is safety. We don't want to expose all beans in the application
 * context, but only explicit beans.
 * <br/>
 * If the 'exposedBeans' bean isn't available, a {@link org.springframework.beans.factory.NoSuchBeanDefinitionException}
 * will be thrown when the application context is created.
 * <p/>
 * All beans that implement {@link HazelcastInstanceAware} will be injected with the HazelcastInstance.
 * <p/>
 * All beans that implement {@link SliceLifecycleListener} will be notified when the slice is starting or stopping,
 * although also the the normal spring init/destroy hooks can be used for that purpose. If a bean is implementing the
 * {@link SliceLifecycleListener} and also the normal Spring functionality is used, it will get multiple calls. So in
 * most cases it is better to make sure that there is only 1 mechanism being used.
 * <p/>
 * If a bean implement {@link HazelcastInstanceAware} and {@link SlicePartitionListener}, the
 * {@link HazelcastInstanceAware#setHazelcastInstance(com.hazelcast.core.HazelcastInstance)} will be called before
 * {@link com.hazelblast.server.SliceLifecycleListener#onStart()}.
 * <p/>
 * All beans that implement {@link SlicePartitionListener} will be notified when a slice starts/ends the management
 * of a partition.
 * <p/>
 * If a bean is created with name 'hazelcastInstance', it will be used as the {@link HazelcastInstance}. If that bean
 * is missing, currently the {@link com.hazelcast.core.Hazelcast#getDefaultInstance()} will be asked for the default
 * {@link HazelcastInstance}.
 *
 * @author Peter Veentjer.
 */
public class SpringSliceFactory implements SliceFactory {

    public Slice create(SliceConfig sliceConfig) {
        return new SpringSlice(sliceConfig);
    }
}
