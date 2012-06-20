package com.hazelblast.client;

import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.loadbalancers.LoadBalancer;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.ExposeService;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadBalanced_DefaultProxyProviderTest {

    private DefaultProxyProvider proxyProvider;
    private SliceServer server;
    private TestService testServiceMock;

    @Before
    public void setUp() throws InterruptedException {
        testServiceMock = mock(TestService.class);
        Pojo pojo = new Pojo();
        pojo.testService = testServiceMock;
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(null);

        PojoSlice slice = new PojoSlice(pojo,hazelcastInstance);

        server = new SliceServer(slice, 100);
        server.start();

        Thread.sleep(1000);

        proxyProvider = new DefaultProxyProvider(hazelcastInstance);
    }

    @After
    public void tearDown() throws InterruptedException {
        try {
            if (server == null) return;
            server.shutdown();
            boolean terminated = server.awaitTermination(10, TimeUnit.SECONDS);
            assertTrue("Could not terminate the service within the given timeout", terminated);
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void notUsableLoadBalancer() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(LoadBalancedMethodWithInvalidLoadBalancer.class);
    }

    @DistributedService
    interface LoadBalancedMethodWithInvalidLoadBalancer {
        @LoadBalanced(loadBalancer = LoadBalancerWithBadConstructor.class)
        void method();
    }

    static class LoadBalancerWithBadConstructor implements LoadBalancer {
        public Member getNext() {
            return null;
        }
    }

    @Test
    public void methodWithoutArguments() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
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
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
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

    static public class Pojo {
        @ExposeService
        public TestService testService;
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
