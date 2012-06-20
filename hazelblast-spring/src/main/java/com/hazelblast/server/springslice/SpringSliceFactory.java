package com.hazelblast.server.springslice;

import com.hazelblast.server.*;
import com.hazelcast.core.HazelcastInstance;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * A Spring based {@link com.hazelblast.server.SliceFactory}.
 * <p/>
 * It expects a slice.xml to be available in the root of the jar.
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
 *
 * @author Peter Veentjer.
 */
public class SpringSliceFactory implements SliceFactory {

    public Slice create(SliceParameters sliceParameters) {
        return new SpringSlice(sliceParameters);
    }

    private static class SpringSlice implements Slice {
        private final ClassPathXmlApplicationContext appContext;
        private final ExposedBeans exposedBeans;
        private final SliceParameters sliceParameters;

        private SpringSlice(SliceParameters sliceParameters) {
            this.appContext = new ClassPathXmlApplicationContext("slice.xml");
            this.exposedBeans = appContext.getBean("exposedBeans", ExposedBeans.class);
            this.sliceParameters = sliceParameters;
        }

        public String getName() {
            return sliceParameters.name;
        }

        public HazelcastInstance getHazelcastInstance() {
            return sliceParameters.hazelcastInstance;
        }

        public Object getService(String name) {
            notNull("name", name);

            if (name.isEmpty()) {
                throw new IllegalArgumentException("name can't be empty");
            }

            name = name.substring(0, 1).toLowerCase() + name.substring(1);
            Object service = exposedBeans.getBean(name);
            if (service == null) {
                throw new IllegalArgumentException(format("service with name '%s' is not exposed", name));
            }

            return service;
        }

        public void onStart() {
            appContext.start();

            String[] names = appContext.getBeanNamesForType(SliceLifecycleAware.class, false, true);

            for (String id : names) {
                SliceLifecycleAware l = (SliceLifecycleAware) appContext.getBean(id);
                l.onStart();
            }
        }

        public void onStop() {
            appContext.close();

            String[] names = appContext.getBeanNamesForType(SliceLifecycleAware.class, false, true);

            for (String id : names) {
                SliceLifecycleAware l = (SliceLifecycleAware) appContext.getBean(id);
                l.onStop();
            }
        }

        public void onPartitionAdded(int partitionId) {
            String[] names = appContext.getBeanNamesForType(SlicePartitionAware.class, false, true);
            for (String id : names) {
                SlicePartitionAware l = (SlicePartitionAware) appContext.getBean(id);
                l.onPartitionAdded(partitionId);
            }
        }

        public void onPartitionRemoved(int partitionId) {
            String[] names = appContext.getBeanNamesForType(SlicePartitionAware.class, false, true);
            for (String id : names) {
                SlicePartitionAware l = (SlicePartitionAware) appContext.getBean(id);
                l.onPartitionRemoved(partitionId);
            }
        }
    }
}
