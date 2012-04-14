package com.hazelblast.server.spring;

import com.hazelcast.hazelblast.api.PuFactory;
import com.hazelcast.hazelblast.api.ProcessingUnit;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * A Spring based {@link PuFactory}.
 *
 * It expects a pu.xml to be available in the root of the jar
 * TODO:
 * (which jar.....).
 */
public class SpringPuFactory implements PuFactory {

    public ProcessingUnit create(int partitionId) {
        return new SpringPu(partitionId);
    }

    private static class SpringPu implements ProcessingUnit {
        private final ClassPathXmlApplicationContext appContext;
        private final int partitionId;

        private SpringPu(int partitionId) {
            this.appContext = new ClassPathXmlApplicationContext("pu.xml");
            this.partitionId = partitionId;
        }

        public Object getService(String name) {
            if(name == null){
                throw new NullPointerException("name can't be null");
            }

            if(name.isEmpty()){
                throw new IllegalArgumentException("name can't be empty");
            }

            name = name.substring(0, 1).toLowerCase() + name.substring(1);
            return appContext.getBean(name);
        }

        public void start() {
            appContext.start();
        }

        public void stop() {
            appContext.stop();
        }
    }
}
