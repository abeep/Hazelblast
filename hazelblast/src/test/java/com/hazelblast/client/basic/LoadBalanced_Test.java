package com.hazelblast.client.basic;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.basic.BasicProxyProvider;
import com.hazelblast.client.router.Router;
import com.hazelblast.client.router.Target;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.*;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadBalanced_Test {

    private BasicProxyProvider proxyProvider;
    private SliceServer server;
    private TestService testServiceMock;
    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() {
        hazelcastInstance = Hazelcast.newHazelcastInstance(null);
    }

    @Before
    public void setUp() throws InterruptedException {
        testServiceMock = mock(TestService.class);

        Pojo pojo = new Pojo(hazelcastInstance);
        pojo.testService = testServiceMock;
        PojoSlice slice = new PojoSlice(pojo);

        server = new SliceServer(slice, 100);
        server.start();

        Thread.sleep(1000);

        proxyProvider = new BasicProxyProvider(hazelcastInstance);
    }

    @After
    public void tearDown() throws InterruptedException {
        TestUtils.shutdownAll(server);
    }

    @AfterClass
    public static void afterClass(){
        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void notUsableLoadBalancer() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        proxyProvider.getProxy(LoadBalancedMethodWithInvalidLoadBalancer.class);
    }

    @DistributedService
    interface LoadBalancedMethodWithInvalidLoadBalancer {
        @LoadBalanced(loadBalancer = LoadBalancerWithBadConstructor.class)
        void method();
    }

    static class LoadBalancerWithBadConstructor implements Router {
        public Target getTarget(Method method, Object[] args) {
            return null;
        }
    }

    @Test
    public void methodWithoutArguments() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        LoadBalancedMethodWithoutArguments p = proxyProvider.getProxy(LoadBalancedMethodWithoutArguments.class);
        assertNotNull(p);
    }

    @DistributedService
    interface LoadBalancedMethodWithoutArguments {
        @LoadBalanced
        void method();
    }

    @Test
    public void methodWithPartitionKeyArgument() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        LoadBalancedMethodWithoutPartitionKeyArgument p = proxyProvider.getProxy(LoadBalancedMethodWithoutPartitionKeyArgument.class);
        assertNotNull(p);
    }

    @DistributedService
    interface LoadBalancedMethodWithoutPartitionKeyArgument {
        @LoadBalanced
        void method(int arg1);
    }

    @Test
    public void whenCalledWithNonNullArgument() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        String result = "result";

        when(testServiceMock.singleArg(arg)).thenReturn(result);

        String found = proxy.singleArg(arg);
        assertEquals(result, found);
    }

    @Test
    public void whenCalledWithNullArgument() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        String result = "result";
        when(testServiceMock.multipleArgs(arg, null)).thenReturn(result);

        String found = proxy.multipleArgs(arg, null);

        assertEquals(result, found);
    }

    static public class Pojo implements HazelcastInstanceProvider {
        @Exposed
        public TestService testService;
        private HazelcastInstance hazelcastInstance;

        public Pojo(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }
    }

    static class MyRuntimeException extends RuntimeException {
    }

    @DistributedService
    interface TestService {
        @LoadBalanced
        String singleArg(@PartitionKey String arg);

        @LoadBalanced
        String multipleArgs(@PartitionKey String arg, String arg2);
    }
}
