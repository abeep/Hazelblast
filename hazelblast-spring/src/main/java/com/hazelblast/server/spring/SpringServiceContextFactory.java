package com.hazelblast.server.spring;

import com.hazelblast.server.ServiceContext;
import com.hazelblast.server.ServiceContextFactory;
import com.hazelblast.server.ServiceContextServer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * A Spring based {@link com.hazelblast.server.ServiceContextFactory}.
 * <p/>
 * It expects a servicecontext.xml to be available in the root of the jar.
 *
 * <h2>exposedBeans bean</h2>
 * The servicecontext.xml should contain a bean called 'exposedBeans' where all the beans that
 * should be exposed remotely, should be registered.
 * <pre>
 * {@code
 *  <bean id="exposedBeans" class="com.hazelblast.server.spring.ExposedBeans">
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
public class SpringServiceContextFactory implements ServiceContextFactory {

    public ServiceContext create() {
        return new SpringServiceContext();
    }

    private static class SpringServiceContext implements ServiceContext {
        private final ClassPathXmlApplicationContext appContext;
        private final ExposedBeans exposedBeans;

        private SpringServiceContext() {
            appContext = new ClassPathXmlApplicationContext("servicecontext.xml");
            exposedBeans = appContext.getBean("exposedBeans",ExposedBeans.class);
        }

        public Object getService(String name) {
            notNull("name",name);

            if (name.isEmpty()) {
                throw new IllegalArgumentException("name can't be empty");
            }

            name = name.substring(0, 1).toLowerCase() + name.substring(1);
            Object service = exposedBeans.getBean(name);
            if(service ==null){
                throw new IllegalArgumentException(format("service with name '%s' is not exposed",name));
            }

            return service;
        }

        public void onStart() {
           appContext.start();
        }

        public void onStop() {
            appContext.close();
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
