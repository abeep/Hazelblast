package com.hazelblast.server.spring;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static java.lang.String.format;

/**
 * A Spring based {@link PuFactory}.
 * <p/>
 * It expects a pu.xml to be available in the root of the jar.
 *
 * <h2>exposedServices bean</h2>
 * The pu.xml should contain a bean called 'exposedServices' where all the services that
 * should be exposed remotely, should be registered.
 * <pre>
 * {@code
 *  <bean id="exposedServices" class="com.hazelblast.server.spring.ExposedServices">
 *      <property name="exposedServices">
 *          <map>
 *              <entry key="employeeService" value-ref="employeeService"/>
 *          </map>
 *      </property>
 *  </bean>
 * </pre>
 * The reason why this is service is there, is even though it will be easier to expose every bean in the
 * application context, for safety purposes we don't want to do that.
 * <br/>
 * If the 'exposedServices' bean isn't available, a {@link org.springframework.beans.factory.NoSuchBeanDefinitionException}
 * will be thrown when the application context is created.
 *
 * @author Peter Veentjer.
 */
public class SpringPuFactory implements PuFactory {

    public ProcessingUnit create() {
        return new SpringPu();
    }

    private static class SpringPu implements ProcessingUnit {
        private final ClassPathXmlApplicationContext appContext;
        private final ExposedServices exposedServices;

        private SpringPu() {
            appContext = new ClassPathXmlApplicationContext("pu.xml");
            exposedServices = appContext.getBean("exposedServices",ExposedServices.class);
        }

        public Object getService(String name) {
            if (name == null) {
                throw new NullPointerException("name can't be null");
            }

            if (name.isEmpty()) {
                throw new IllegalArgumentException("name can't be empty");
            }

            name = name.substring(0, 1).toLowerCase() + name.substring(1);
            Object service = exposedServices.getService(name);
            if(service ==null){
                throw new IllegalArgumentException(format("service with name '%s' is not exposed",name));
            }

            return service;
        }

        public void onStart() {
            appContext.start();
        }

        public void onStop() {
            appContext.stop();
        }

        public void onPartitionAdded(int partitionId) {
            String[] names = appContext.getBeanNamesForType(SpringPartitionListener.class, false, true);
            for (String id : names) {
                SpringPartitionListener l = (SpringPartitionListener) appContext.getBean(id);
                l.onPartitionAdded(partitionId);
            }
        }

        public void onPartitionRemoved(int partitionId) {
            String[] names = appContext.getBeanNamesForType(SpringPartitionListener.class, false, true);
            for (String id : names) {
                SpringPartitionListener l = (SpringPartitionListener) appContext.getBean(id);
                l.onPartitionRemoved(partitionId);
            }
        }
    }
}
