package com.hazelblast.server.spring;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A Spring based {@link PuFactory}.
 * <p/>
 * It expects a pu.xml to be available in the root of the jar
 * TODO:
 * (which jar.....).
 */
public class SpringPuFactory implements PuFactory {

    public ProcessingUnit create() {
        return new SpringPu();
    }

    private static class SpringPu implements ProcessingUnit {
        private final ClassPathXmlApplicationContext appContext;

        private SpringPu() {
            this.appContext = new ClassPathXmlApplicationContext("pu.xml");
        }

        public Object getService(String name) {
            if (name == null) {
                throw new NullPointerException("name can't be null");
            }

            if (name.isEmpty()) {
                throw new IllegalArgumentException("name can't be empty");
            }

            name = name.substring(0, 1).toLowerCase() + name.substring(1);
            return appContext.getBean(name);
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
