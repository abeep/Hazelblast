package com.hazelblast.server.springslice;

import com.hazelblast.server.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.partition.Partition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
        private static Log log = LogFactory.getLog(SpringSlice.class);

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
                throw new IllegalArgumentException(format("Service with name '%s' is not found", name));
            }

            return service;
        }

        public void onStart() {
            for (String id : appContext.getBeanNamesForType(HazelcastInstanceAware.class, false, true)) {
                HazelcastInstanceAware l = (HazelcastInstanceAware) appContext.getBean(id);
                try {
                    l.setHazelcastInstance(sliceParameters.hazelcastInstance);
                } catch (Exception e) {
                    log.warn(format("failed to call setHazelcastInstance on bean [%s] of class [%s]", id, l.getClass()), e);
                }
            }

            appContext.start();

            for (String id : appContext.getBeanNamesForType(SliceLifecycleListener.class, false, true)) {
                SliceLifecycleListener l = (SliceLifecycleListener) appContext.getBean(id);
                try {
                    l.onStart();
                } catch (Exception e) {
                    log.warn(format("failed to call onStart on bean [%s] of class [%s]", id, l.getClass()), e);
                }
            }
        }

        public void onStop() {
            appContext.close();

            String[] names = appContext.getBeanNamesForType(SliceLifecycleListener.class, false, true);
            for (String id : names) {
                SliceLifecycleListener l = (SliceLifecycleListener) appContext.getBean(id);
                try {
                    l.onStop();
                } catch (Exception e) {
                    log.warn(format("failed to call onStop on bean [%s] of class [%s]", id, l.getClass()), e);
                }
            }
        }

        public void onPartitionAdded(Partition partition) {
            for (String id : appContext.getBeanNamesForType(SlicePartitionListener.class, false, true)) {
                SlicePartitionListener l = (SlicePartitionListener) appContext.getBean(id);
                try {
                    l.onPartitionAdded(partition);
                } catch (Exception e) {
                    log.warn(format("failed to call onPartitionAdded on bean [%s] of class [%s]", id, l.getClass()), e);
                }
            }
        }

        public void onPartitionRemoved(Partition partition) {
             for (String id : appContext.getBeanNamesForType(SlicePartitionListener.class, false, true)) {
                SlicePartitionListener l = (SlicePartitionListener) appContext.getBean(id);
                try {
                    l.onPartitionRemoved(partition);
                } catch (Exception e) {
                    log.warn(format("failed to call onPartitionRemoved on bean [%s] of class [%s]", id, l.getClass()), e);
                }
            }
        }
    }
}
