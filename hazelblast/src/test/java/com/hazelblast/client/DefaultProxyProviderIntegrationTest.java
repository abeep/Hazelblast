package com.hazelblast.client;


import com.hazelblast.api.LoadBalanced;
import com.hazelblast.api.PartitionKey;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RemoteInterface;
import com.hazelblast.server.ServiceContextServer;
import com.hazelblast.server.pojo.PojoServiceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.fail;

public class DefaultProxyProviderIntegrationTest {

    private DefaultProxyProvider proxyProvider;
    private ServiceContextServer server;
    private TestService testServiceMock;

    @Before
    public void setUp() {
        testServiceMock = createMock(TestService.class);
        Pojo pojo = new Pojo();
        pojo.testService = testServiceMock;
        PojoServiceContext serviceContext = new PojoServiceContext(pojo);
        server = new ServiceContextServer(serviceContext, "default");
        server.start();
        proxyProvider = new DefaultProxyProvider();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (server == null) return;
        server.shutdown();
        boolean success = server.awaitTermination(10, TimeUnit.SECONDS);
        if (!success) throw new RuntimeException("server awaitTermination timed out");
    }

    @Test
    public void exceptionUnwrappingPartitionedMethod() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        expect(testServiceMock.singleArg(arg)).andThrow(new MyRuntimeException());

        replay(testServiceMock);

        try {
            proxy.singleArg(arg);
            fail();
        } catch (MyRuntimeException expected) {

        }
        verify(testServiceMock);
    }

    @Test
    public void exceptionUnwrappingLoadBalancedMethod() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        expect(testServiceMock.loadBalanced(arg)).andThrow(new MyRuntimeException());

        replay(testServiceMock);

        try {
            proxy.loadBalanced(arg);
            fail();
        } catch (MyRuntimeException expected) {

        }
        verify(testServiceMock);
    }

    @Test
    @Ignore
    public void exceptionUnwrappingForkJoinMethod() {

    }


    @Test
    public void whenCalledWithNonNullArgument() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        String result = "result";
        expect(testServiceMock.singleArg(arg)).andReturn(result);

        replay(testServiceMock);

        proxy.singleArg(arg);

        verify(testServiceMock);
    }

    @Test
    @Ignore
    public void whenCalledWithNullArgument() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = null;
        String result = "result";
        expect(testServiceMock.singleArg(arg)).andReturn(result);


        replay(testServiceMock);

        proxy.multipleArgs(arg, null);

        verify(testServiceMock);
    }

    static public class Pojo {
        public TestService testService;
    }

    static class MyRuntimeException extends RuntimeException {
    }

    @RemoteInterface
    interface TestService {
        @Partitioned
        String singleArg(@PartitionKey String arg);

        @Partitioned
        String multipleArgs(@PartitionKey String arg, String arg2);

        @LoadBalanced
        String loadBalanced(String arg);

    }
}
