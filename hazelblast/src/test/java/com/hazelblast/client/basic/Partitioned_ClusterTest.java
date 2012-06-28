package com.hazelblast.client.basic;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
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

public class Partitioned_ClusterTest {

    @Before
    public void before() {
        Hazelcast.shutdownAll();
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void whenOptimizeLocalCall() throws Throwable {
        test(true);
    }

    @Test
    public void whenNotOptimizeLocalCall() throws Throwable {
        test(true);
    }

    public void test(boolean optimize) throws Throwable {
        HazelcastInstance instance1 = TestUtils.newServerInstance();
        HazelcastInstance instance2 = TestUtils.newServerInstance();
        HazelcastInstance instance3 = TestUtils.newServerInstance();

        PojoSlice slice1 = new PojoSlice(new Pojo(instance1));
        PojoSlice slice2 = new PojoSlice(new Pojo(instance2));
        PojoSlice slice3 = new PojoSlice(new Pojo(instance3));

        SomeServiceImpl service1 = (SomeServiceImpl) slice1.getService("someService");
        SomeServiceImpl service2 = (SomeServiceImpl) slice2.getService("someService");
        SomeServiceImpl service3 = (SomeServiceImpl) slice3.getService("someService");

        SliceServer server1 = build(slice1);
        SliceServer server2 = build(slice2);
        SliceServer server3 = build(slice3);

        HazelcastInstance clientInstance = optimize ? instance1 : TestUtils.newLiteInstance();

        BasicProxyProvider proxyProvider = new BasicProxyProvider(clientInstance);
        proxyProvider.setLocalCallOptimizationEnabled(optimize);

        SomeService someService = proxyProvider.getProxy(SomeService.class);

        int callPerInstance = 1000;

        for (int k = 0; k < 3 * callPerInstance; k++) {
            someService.someMethod(k);
        }

        int sum = service1.count + service2.count + service3.count;
        assertEquals(callPerInstance * 3, sum);

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
