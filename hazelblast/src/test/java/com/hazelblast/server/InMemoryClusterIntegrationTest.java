package com.hazelblast.server;

import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelblast.server.pojoslice.PojoSliceFactory;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InMemoryClusterIntegrationTest {

    @Before
    public void before() {
        Hazelcast.shutdownAll();
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() throws Throwable {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(null);

        PojoSliceFactory factory = new PojoSliceFactory(Pojo.class);

        PojoSlice slice1 = factory.create();
        PojoSlice slice2 = factory.create();
        PojoSlice slice3 = factory.create();

        SomeService service1 = (SomeService) slice1.getService("someService");
        SomeService service2 = (SomeService) slice2.getService("someService");
        SomeService service3 = (SomeService) slice3.getService("someService");

        SliceServer server1 = build(slice1, instance1, "foo");
        SliceServer server2 = build(slice2, instance2, "foo");
        SliceServer server3 = build(slice3, instance3, "foo");

        SliceServer.executeMethod(instance1, "foo", "SomeService", "someMethod", new String[]{}, new Object[]{}, null);
        SliceServer.executeMethod(instance2, "foo", "SomeService", "someMethod", new String[]{}, new Object[]{}, null);
        SliceServer.executeMethod(instance2, "foo", "SomeService", "someMethod", new String[]{}, new Object[]{}, null);

        assertEquals(1, service1.count);
        assertEquals(2, service2.count);
        assertEquals(0, service3.count);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
    }

    public SliceServer build(Slice slice, HazelcastInstance hazelcastInstance, String name) {
        SliceServer server = new SliceServer(slice, name, 1000, hazelcastInstance);
        server.start();
        return server;
    }

    public static class Pojo {
        public SomeService someService = new SomeService();

        public Pojo() {
        }
    }

    public static class SomeService {
        public int count;

        public void someMethod() {
            count++;
        }
    }
}
