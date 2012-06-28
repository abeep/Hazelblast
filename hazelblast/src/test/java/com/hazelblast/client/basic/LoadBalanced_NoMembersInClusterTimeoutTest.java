package com.hazelblast.client.basic;

import com.hazelblast.TestUtils;
import com.hazelblast.client.ProxyProvider;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.exceptions.DistributedMethodTimeoutException;
import com.hazelblast.client.router.RoundRobinLoadBalancer;
import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelblast.TestUtils.newLiteInstance;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LoadBalanced_NoMembersInClusterTimeoutTest {

    private static HazelcastInstance liteMember;
    private static volatile SliceServer sliceServer;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
        liteMember = newLiteInstance();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void before() {
        sliceServer = null;
    }

    @After
    public void after() throws InterruptedException {
        TestUtils.shutdownAll(sliceServer);
    }

    @Test
    public void whenNoMemberInClusterThenTimeout() {
        ProxyProvider proxyProvider = new BasicProxyProvider(liteMember);
        TestService testService = proxyProvider.getProxy(TestService.class);

        try {
            testService.fiveSecondTimeout();
            fail();
        } catch (DistributedMethodTimeoutException e) {

        }
    }

    @Test
    public void whenNoMemberStartsAfterSomeTime_thenCallIsExecuted() {
        ProxyProvider proxyProvider = new BasicProxyProvider(liteMember);
        TestService testService = proxyProvider.getProxy(TestService.class);

        new Thread(new StartSliceServerTask(10000)).start();

        testService.sixtySecondTimeout();
    }

    class StartSliceServerTask implements Runnable {
        final int delayMs;

        StartSliceServerTask(int delayMs) {
            this.delayMs = delayMs;
        }

        public void run() {
            try {
                System.out.printf("Waiting %s ms\n",delayMs);
                Thread.sleep(delayMs);
                System.out.println("Starting sliceServer");

                HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
                Slice slice = new PojoSlice(new Pojo(instance));
                sliceServer = new SliceServer(slice).start();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @DistributedService
    interface TestService {
        @LoadBalanced(timeoutMs = 5000, interruptOnTimeout = true, loadBalancer = RoundRobinLoadBalancer.class)
        void fiveSecondTimeout();

        @LoadBalanced(timeoutMs = 60000, interruptOnTimeout = true, loadBalancer = RoundRobinLoadBalancer.class)
        void sixtySecondTimeout();
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

    public static class TestServiceImpl implements TestService {
        public final AtomicBoolean interrupted = new AtomicBoolean(false);

        public TestServiceImpl() {
            System.out.println("TestServiceImpl created");
        }

        public void fiveSecondTimeout() {
        }

        public void sixtySecondTimeout() {
        }
    }
}
