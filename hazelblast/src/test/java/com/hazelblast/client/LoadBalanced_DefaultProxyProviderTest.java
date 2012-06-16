package com.hazelblast.client;

import com.hazelblast.api.LoadBalanced;
import com.hazelblast.api.PartitionKey;
import com.hazelblast.api.RemoteInterface;
import com.hazelblast.api.LoadBalancer;
import com.hazelblast.server.ServiceContextServer;
import com.hazelblast.server.pojo.PojoServiceContext;
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
    private ServiceContextServer server;
    private TestService testServiceMock;

    @Before
    public void setUp() throws InterruptedException {
        testServiceMock = mock(TestService.class);
        Pojo pojo = new Pojo();
        pojo.testService = testServiceMock;
        PojoServiceContext serviceContext = new PojoServiceContext(pojo);
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(null);

        server = new ServiceContextServer(serviceContext, "default", 100, hazelcastInstance);
        server.start();

        Thread.sleep(1000);

        proxyProvider = new DefaultProxyProvider("default", hazelcastInstance);
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

    @RemoteInterface
    interface LoadBalancedMethodWithInvalidLoadBalancer {
        @LoadBalanced(loadBalancer = LoadBalancerWithBadConstructor.class)
        void method();
    }

    static class LoadBalancerWithBadConstructor implements LoadBalancer {
        public Member findTargetMember() {
            return null;
        }
    }

    @Test
    public void methodWithoutArguments() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        LoadBalancedMethodWithoutArguments p = proxyProvider.getProxy(LoadBalancedMethodWithoutArguments.class);
        assertNotNull(p);
    }

    @RemoteInterface
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

    @RemoteInterface
    interface LoadBalancedMethodWithoutPartitionKeyArgument {
        @LoadBalanced
        void method(int arg1);
    }

    @Test
    public void exceptionUnwrapping() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        when(testServiceMock.singleArg(arg)).thenThrow(new MyRuntimeException());

        try {
            proxy.singleArg(arg);
            fail();
        } catch (MyRuntimeException expected) {
        }
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
        public TestService testService;
    }

    static class MyRuntimeException extends RuntimeException {
    }

    @RemoteInterface
    interface TestService {
        @LoadBalanced
        String singleArg(@PartitionKey String arg);

        @LoadBalanced
        String multipleArgs(@PartitionKey String arg, String arg2);
    }
}
