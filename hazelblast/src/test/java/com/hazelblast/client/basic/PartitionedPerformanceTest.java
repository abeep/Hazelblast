package com.hazelblast.client.basic;

import com.hazelblast.client.ProxyProvider;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.router.RoundRobinLoadBalancer;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class PartitionedPerformanceTest {

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
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);

        PojoSlice slice = new PojoSlice(new Pojo(instance));

        new SliceServer(slice).start();

        ProxyProvider proxyProvider = new BasicProxyProvider(instance);
        SomeService someService = proxyProvider.getProxy(SomeService.class);

        System.out.println("Starting");
        long startMs = System.currentTimeMillis();
        long callCount = 100 * 1000;

        for (int k = 0; k < callCount; k++) {
            someService.someMethod("foo");
            if (k % 1000 == 0) {
                System.out.printf("at %s\n",k);
            }
        }

        long durationMs = System.currentTimeMillis() - startMs;
        System.out.printf("Finished in %s ms\n", durationMs);

        double callsPerSecond = (callCount * 1000d) / durationMs;
        System.out.println("Performance is " + callsPerSecond+" calls per second");
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
        @Partitioned
        void someMethod(@PartitionKey String s);
    }

    public static class SomeServiceImpl implements SomeService {
        public int count;


        public void someMethod(String s) {
            count++;
        }
    }
}
