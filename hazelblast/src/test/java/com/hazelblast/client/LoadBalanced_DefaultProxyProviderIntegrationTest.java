package com.hazelblast.client;


import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelblast.server.pojoslice.ExposeService;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadBalanced_DefaultProxyProviderIntegrationTest {

    private DefaultProxyProvider proxyProvider;
    private SliceServer server;
    private TestService testServiceMock;

    @Before
    public void setUp() throws InterruptedException {
        testServiceMock = mock(TestService.class);
        Pojo pojo = new Pojo();
        pojo.testService = testServiceMock;
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(null);

        PojoSlice slice = new PojoSlice(pojo,"default",hazelcastInstance);

        server = new SliceServer(slice, 100);
        server.start();

        Thread.sleep(1000);

        proxyProvider = new DefaultProxyProvider( hazelcastInstance);
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
    public void exceptionUnwrapping() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        when(testServiceMock.singleArg(arg)).thenThrow(new MyRuntimeException());

        try {
            proxy.singleArg(arg);
            fail();
        } catch (MyRuntimeException expected) {
            System.err.println("---------------------------------------------------");
            expected.printStackTrace();
            System.err.println("---------------------------------------------------");
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

    @Test
    public void whenNoArguments() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String result = "result";
        when(testServiceMock.noArgs()).thenReturn(result);

        String found = proxy.noArgs();

        assertEquals(result, found);
    }

    static public class Pojo {
        @ExposeService
        public TestService testService;
    }

    static class MyRuntimeException extends RuntimeException {
        MyRuntimeException() {
        }

        MyRuntimeException(String message) {
            super(message);
        }

        MyRuntimeException(String message, Throwable cause) {
            super(message, cause);
        }

        MyRuntimeException(Throwable cause) {
            super(cause);
        }
    }

    @DistributedService
    interface TestService {
        @LoadBalanced
        String noArgs();

        @LoadBalanced
        String singleArg(String arg);

        @LoadBalanced
        String multipleArgs(String arg, String arg2);
    }
}
