package com.hazelblast.client.impl;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.exceptions.DistributedMethodTimeoutException;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.*;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class Partitioned_TimeoutIntegrationTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
        hazelcastInstance = Hazelcast.newHazelcastInstance(null);
    }

    @AfterClass
    public static void afterClass(){
        Hazelcast.shutdownAll();
    }

    private ProxyProviderImpl proxyProvider;
    private SliceServer server;
    private Pojo pojo;

    @Before
    public void setUp() throws InterruptedException {
        pojo = new Pojo(hazelcastInstance);
        PojoSlice slice = new PojoSlice(pojo);

        server = new SliceServer(slice, 100);
        server.start();

        Thread.sleep(1000);

        proxyProvider = new ProxyProviderImpl(hazelcastInstance);
        proxyProvider.setLocalCallOptimizationEnabled(false);
    }

    @After
    public void tearDown() throws InterruptedException {
        TestUtils.shutdownAll(server);
    }

    @Test
    public void whenCallDoesntTimeout() {
        TestService testService = proxyProvider.getProxy(TestService.class);
        testService.fiveSecondTimeoutAndInterruptible("somepartition", 1000);
    }

    @Test
    public void whenTimeoutAndInterruptible() throws InterruptedException {
        TestService testService = proxyProvider.getProxy(TestService.class);

        try {
            testService.fiveSecondTimeoutAndInterruptible("somepartition", 6000);
            fail();
        } catch (DistributedMethodTimeoutException expected) {

        }
        Thread.sleep(2000);

        assertTrue(pojo.testService.interrupted.get());
    }

    @Test
    public void whenTimeoutNotInterruptible() throws InterruptedException {
        TestService testService = proxyProvider.getProxy(TestService.class);

        try {
            testService.fiveSecondTimeoutNotInterruptible("somepartition", 10000);
            fail();
        } catch (DistributedMethodTimeoutException expected) {

        }
        Thread.sleep(10000);

        assertFalse(pojo.testService.interrupted.get());
    }

    static public class Pojo implements HazelcastInstanceProvider {
        @Exposed
        public TestServiceImpl testService = new TestServiceImpl();

        private final HazelcastInstance hazelcastInstance;

        public Pojo(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }
    }

    @DistributedService
    interface TestService {
        @Partitioned(timeoutMs = 5000, interruptOnTimeout = true)
        void fiveSecondTimeoutAndInterruptible(@PartitionKey String p, int timeoutMs);

        @Partitioned(timeoutMs = 5000, interruptOnTimeout = false)
        void fiveSecondTimeoutNotInterruptible(@PartitionKey String p, int timeoutMs);
    }

    public static class TestServiceImpl implements TestService {
        public final AtomicBoolean interrupted = new AtomicBoolean(false);

        public TestServiceImpl() {
            System.out.println("TestServiceImpl created");
        }

        public void fiveSecondTimeoutAndInterruptible(String p, int timeoutMs) {
            Thread.interrupted();
            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }

        public void fiveSecondTimeoutNotInterruptible(String p, int timeoutMs) {
            Thread.interrupted();

            try {
                Thread.sleep(timeoutMs);
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }
    }
}
