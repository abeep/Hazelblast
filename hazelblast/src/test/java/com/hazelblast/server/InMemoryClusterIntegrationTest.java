package com.hazelblast.server;

import com.hazelblast.server.pojoslice.ExposeService;
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

        PojoSlice slice1 = factory.create(new SliceParameters(instance1));
        PojoSlice slice2 = factory.create(new SliceParameters(instance2));
        PojoSlice slice3 = factory.create(new SliceParameters(instance3));

        SomeService service1 = (SomeService) slice1.getService("someService");
        SomeService service2 = (SomeService) slice2.getService("someService");
        SomeService service3 = (SomeService) slice3.getService("someService");

        SliceServer server1 = new SliceServer(slice1, 1000).start();
        SliceServer server2 = new SliceServer(slice2, 1000).start();
        SliceServer server3 = new SliceServer(slice3, 1000).start();

        SliceServer.executeMethod(instance1, Slice.DEFAULT_NAME, "SomeService", "someMethod", new String[]{}, new Object[]{}, null);
        SliceServer.executeMethod(instance2, Slice.DEFAULT_NAME, "SomeService", "someMethod", new String[]{}, new Object[]{}, null);
        SliceServer.executeMethod(instance2, Slice.DEFAULT_NAME, "SomeService", "someMethod", new String[]{}, new Object[]{}, null);

        assertEquals(1, service1.count);
        assertEquals(2, service2.count);
        assertEquals(0, service3.count);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
    }

    public static class Pojo {
        @ExposeService
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
