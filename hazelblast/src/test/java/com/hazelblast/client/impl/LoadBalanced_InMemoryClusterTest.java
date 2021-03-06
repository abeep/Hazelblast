package com.hazelblast.client.impl;

import com.hazelblast.TestUtils;
import com.hazelblast.client.ProxyProvider;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.router.RoundRobinLoadBalancer;
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

public class LoadBalanced_InMemoryClusterTest {

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

        ProxyProvider proxyProvider = new ProxyProviderImpl(clientInstance);
        SomeService someService = proxyProvider.getProxy(SomeService.class);

        int countPerPartition = 10000;

        long startMs = System.currentTimeMillis();
        System.out.println("Starting");
        for (int k = 0; k < 3 * countPerPartition; k++) {
            someService.someMethod();

            if(k%1000==0){
                System.out.printf("at %s\n",k);
            }
        }

        long durationMs = System.currentTimeMillis()-startMs;
        System.out.printf("Finished in %s ms\n",durationMs);

        assertEquals(countPerPartition, service1.count);
        assertEquals(countPerPartition, service2.count);
        assertEquals(countPerPartition, service3.count);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
    }

    public SliceServer build(Slice slice) {
        SliceServer server = new SliceServer(slice, 1000);
        server.start();
        return server;
    }

    public static class Pojo implements HazelcastInstanceProvider {
        @Exposed
        public final SomeService someService = new SomeServiceImpl();
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
        @LoadBalanced(loadBalancer = RoundRobinLoadBalancer.class)
        void someMethod();
    }

    public static class SomeServiceImpl implements SomeService {
        public int count;


        public void someMethod() {
            count++;
        }
    }
}
