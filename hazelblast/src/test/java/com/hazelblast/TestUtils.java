package com.hazelblast;

import com.hazelblast.server.SliceServer;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

public class TestUtils {

    public static void shutdownAll(SliceServer... servers) {
        for (SliceServer server : servers) {
            if (server != null) {
                server.shutdown();
                try {
                    boolean terminated = server.awaitTermination(10, TimeUnit.SECONDS);
                    assertTrue("Could not terminate the service within the given timeout", terminated);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

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

    public static void assertContainsText(String s, String contains) {
        int indexOf = s.indexOf(contains);
        assertTrue(indexOf >= 0);
    }

    public static String toString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }

    public static void scheduleSetFalse(final AtomicBoolean atomicBoolean, final long delaysMs) {
        new Thread() {
            public void run() {
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
