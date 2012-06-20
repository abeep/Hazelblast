package com.hazelblast.client;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.exceptions.RemoteMethodTimeoutException;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelblast.server.pojoslice.ExposeService;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class LoadBalanced_Timeout_ProxyProviderIntegrationTest {
    private DefaultProxyProvider proxyProvider;
    private SliceServer server;
    private Pojo pojo;

    @Before
    public void setUp() throws InterruptedException {
        Hazelcast.shutdownAll();

        pojo = new Pojo();
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(null);
        PojoSlice slice = new PojoSlice(pojo,hazelcastInstance);

        server = new SliceServer(slice, 100);
        server.start();

        Thread.sleep(1000);

        HazelcastInstance clientInstance = TestUtils.newLiteInstance();
        proxyProvider = new DefaultProxyProvider("default", clientInstance);
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
        testService.fiveSecondTimeoutAndInterruptible(1000);
    }

    @Test
    public void whenTimeoutAndInterruptible() throws InterruptedException {
        TestService testService = proxyProvider.getProxy(TestService.class);

        try {
            testService.fiveSecondTimeoutAndInterruptible(6000);
            fail();
        } catch (RemoteMethodTimeoutException expected) {

        }
        Thread.sleep(2000);

        assertTrue(pojo.testService.interrupted.get());
    }

    @Test
    public void whenTimeoutNotInterruptible() throws InterruptedException {
        TestService testService = proxyProvider.getProxy(TestService.class);

        try {
            testService.fiveSecondTimeoutNotInterruptible(10000);
            fail();
        } catch (RemoteMethodTimeoutException expected) {

        }
        Thread.sleep(10000);

        assertFalse(pojo.testService.interrupted.get());
    }


    static public class Pojo {
        @ExposeService
        public TestServiceImpl testService = new TestServiceImpl();
    }

    @DistributedService
    interface TestService {
        @LoadBalanced(timeoutMs = 5000, interruptOnTimeout = true)
        void fiveSecondTimeoutAndInterruptible(int timeoutMs);

        @LoadBalanced(timeoutMs = 5000, interruptOnTimeout = false)
        void fiveSecondTimeoutNotInterruptible(int timeoutMs);
    }

    public static class TestServiceImpl implements TestService {
        public final AtomicBoolean interrupted = new AtomicBoolean(false);

        public TestServiceImpl() {
            System.out.println("TestServiceImpl created");
        }

        public void fiveSecondTimeoutAndInterruptible(int timeoutMs) {
            Thread.interrupted();
            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }

        public void fiveSecondTimeoutNotInterruptible(int timeoutMs) {
            Thread.interrupted();

            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }
    }
}
