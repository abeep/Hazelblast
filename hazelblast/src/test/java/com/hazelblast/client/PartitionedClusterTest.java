package com.hazelblast.client;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.smarter.DefaultProxyProvider;
import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
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

        PojoSlice slice1 = new PojoSlice(new Pojo(instance1));
        PojoSlice slice2 = new PojoSlice(new Pojo(instance2));
        PojoSlice slice3 = new PojoSlice(new Pojo(instance3));

        SomeServiceImpl service1 = (SomeServiceImpl) slice1.getService("someService");
        SomeServiceImpl service2 = (SomeServiceImpl) slice2.getService("someService");
        SomeServiceImpl service3 = (SomeServiceImpl) slice3.getService("someService");

        SliceServer server1 = build(slice1);
        SliceServer server2 = build(slice2);
        SliceServer server3 = build(slice3);

        HazelcastInstance clientInstance = TestUtils.newLiteInstance();

        ProxyProvider proxyProvider = new DefaultProxyProvider(clientInstance);
        SomeService someService = proxyProvider.getProxy(SomeService.class);

        int callPerInstance = 1000;

        for (int k = 0; k < 3 * callPerInstance; k++) {
            someService.someMethod(k);
        }

        int sum = service1.count + service2.count + service3.count;
        assertEquals(callPerInstance*3, sum);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
    }

    public SliceServer build(Slice slice) {
        SliceServer server = new SliceServer(slice, 1000);
        return server.start();
    }

    public static class Pojo implements HazelcastInstanceProvider {
        @Exposed
        public SomeService someService = new SomeServiceImpl();
        private final HazelcastInstance hazelcastInstance;

        public Pojo(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }
    }

    @DistributedService
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
