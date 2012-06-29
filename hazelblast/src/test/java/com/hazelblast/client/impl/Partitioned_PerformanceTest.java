package com.hazelblast.client.impl;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelblast.testcategories.PerformanceTest;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicBoolean;


@Category(PerformanceTest.class)
public class Partitioned_PerformanceTest {

    private AtomicBoolean run;

    @Before
    public void before() {
        Hazelcast.shutdownAll();
        run = new AtomicBoolean(true);
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void whenOptimizeLocalCalls() throws Throwable {
        test(true);
    }

    @Test
    public void whenNotOptimizeLocalCalls() throws Throwable {
        test(false);
    }

    public void test(boolean optimized) throws Throwable {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);

        PojoSlice slice = new PojoSlice(new Pojo(instance));

        new SliceServer(slice).start();

        ProxyProviderImpl proxyProvider = new ProxyProviderImpl(instance);
        proxyProvider.setLocalCallOptimizationEnabled(optimized);
        SomeService someService = proxyProvider.getProxy(SomeService.class);
        //do an initial call to make sure everything is up and running.
        someService.someMethod("foo");

        System.out.printf("Starting with localCallOptimizationEnabled: %s\n", optimized);
        TestUtils.scheduleSetFalse(run, 30 * 1000);
        long startMs = System.currentTimeMillis();

        long count = 0;
        while (run.get()) {
            count++;
            someService.someMethod("foo");
        }

        long durationMs = System.currentTimeMillis() - startMs;
        System.out.printf("Finished in %s ms\n", durationMs);
        System.out.printf("Total number of calls: %s\n", count);
        double callsPerSecond = (count * 1000d) / durationMs;
        System.out.printf("Performance is %s calls per second\n", callsPerSecond);
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
