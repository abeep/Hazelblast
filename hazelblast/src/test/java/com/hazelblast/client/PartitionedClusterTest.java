package com.hazelblast.client;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.annotations.RemoteInterface;
import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelblast.server.pojoslice.PojoSliceFactory;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PartitionedClusterTest {

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

        SomeServiceImpl service1 = (SomeServiceImpl) slice1.getService("someService");
        SomeServiceImpl service2 = (SomeServiceImpl) slice2.getService("someService");
        SomeServiceImpl service3 = (SomeServiceImpl) slice3.getService("someService");

        SliceServer server1 = build(slice1, instance1, "foo");
        SliceServer server2 = build(slice2, instance2, "foo");
        SliceServer server3 = build(slice3, instance3, "foo");

        HazelcastInstance clientInstance = TestUtils.newLiteInstance();

        ProxyProvider proxyProvider = new DefaultProxyProvider("foo", clientInstance);
        SomeService someService = proxyProvider.getProxy(SomeService.class);

        for (int k = 0; k < 3 * 5; k++) {
            someService.someMethod(k);
        }

        //assertEquals(5, service1.count);
        //assertEquals(5, service2.count);
        //assertEquals(5, service3.count);

        int sum = service1.count + service2.count + service3.count;
        assertEquals(15, sum);

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
        public SomeService someService = new SomeServiceImpl();

        public Pojo() {
        }
    }

    @RemoteInterface
    public static interface SomeService {
        @Partitioned
        void someMethod(@PartitionKey int x);
    }

    public static class SomeServiceImpl implements SomeService {
        public int count;


        public void someMethod(int x) {
            count++;
        }
    }
}
