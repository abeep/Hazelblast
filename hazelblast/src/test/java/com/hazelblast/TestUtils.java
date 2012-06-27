package com.hazelblast;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestUtils {


    public static HazelcastInstance newLiteInstance() {
        Config config = new XmlConfigBuilder().build();
        config.setLiteMember(true);
        return Hazelcast.newHazelcastInstance(config);
    }

    public static Collection<HazelcastInstance> newInstances(int count) {
        List<HazelcastInstance> result = new LinkedList<HazelcastInstance>();
        for (int k = 0; k < count; k++) {
            result.add(Hazelcast.newHazelcastInstance(null));
        }
        return null;
    }

    public static void shutdownAll(HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().shutdown();
        }
    }

    public static void shutdownAll(Collection<HazelcastInstance> instances) {
        for (HazelcastInstance instance : instances) {
            instance.getLifecycleService().shutdown();
        }
    }

    public static HazelcastInstance newServerInstance() {

        return Hazelcast.newHazelcastInstance(null);
    }

    public static void scheduleSetFalse(final AtomicBoolean atomicBoolean, final long delaysMs){
         new Thread(){
             public void run(){
                 try {
                     Thread.sleep(delaysMs);
                     atomicBoolean.set(false);
                 } catch (InterruptedException e) {
                     e.printStackTrace();
                 }
             }
         }.start();
    }
}
