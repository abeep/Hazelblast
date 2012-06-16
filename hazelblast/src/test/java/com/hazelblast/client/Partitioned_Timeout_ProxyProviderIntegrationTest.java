package com.hazelblast.client;

import com.hazelblast.api.PartitionKey;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RemoteInterface;
import com.hazelblast.api.exceptions.RemoteMethodTimeoutException;
import com.hazelblast.server.ServiceContextServer;
import com.hazelblast.server.pojo.PojoServiceContext;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class Partitioned_Timeout_ProxyProviderIntegrationTest {
    private DefaultProxyProvider proxyProvider;
    private ServiceContextServer server;

    @Before
    public void setUp() throws InterruptedException {
        Pojo pojo = new Pojo();
        PojoServiceContext serviceContext = new PojoServiceContext(pojo);
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(null);

        server = new ServiceContextServer(serviceContext, "default", 100, hazelcastInstance);
        server.start();

        Thread.sleep(1000);

        proxyProvider = new DefaultProxyProvider("default", hazelcastInstance.getExecutorService());
    }

    @After
    public void tearDown() throws InterruptedException {
        if (server == null) return;
        server.shutdown();
        boolean terminated = server.awaitTermination(10, TimeUnit.SECONDS);
        assertTrue("Could not terminate the servce within the given timeout", terminated);
        Hazelcast.shutdownAll();
    }

    @Test
    public void whenCallDoesntTimeout() {
        TestService testService = proxyProvider.getProxy(TestService.class);
        testService.fiveSecondTimeout("somepartition",1000);
    }

    @Test(expected = RemoteMethodTimeoutException.class)
    public void whenTimeout() {
        TestService testService = proxyProvider.getProxy(TestService.class);

        testService.fiveSecondTimeout("somepartition",6000);
    }

    static public class Pojo {
        public TestService testService = new TestServiceImpl();
    }

    @RemoteInterface
    interface TestService {
        @Partitioned(timeoutMs = 5000)
        void fiveSecondTimeout(@PartitionKey String partition, int timeoutMs);
    }

    public static class TestServiceImpl implements TestService {

        public void fiveSecondTimeout(String partition, int timeoutMs) {
            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
            }
        }
    }
}
