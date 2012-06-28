package com.hazelblast.client.impl;


import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.*;

import static com.hazelblast.TestUtils.assertContainsText;
import static org.junit.Assert.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class LoadBalanced_IntegrationTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
        hazelcastInstance = TestUtils.newServerInstance();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    private ProxyProviderImpl proxyProvider;
    private SliceServer server;
    private TestService testServiceMock;

    @Before
    public void setUp() throws InterruptedException {
        testServiceMock = mock(TestService.class);

        Pojo pojo = new Pojo(hazelcastInstance);
        pojo.testService = testServiceMock;

        PojoSlice slice = new PojoSlice(pojo);
        server = new SliceServer(slice, 100).start();

        proxyProvider = new ProxyProviderImpl(hazelcastInstance);
        proxyProvider.setLocalCallOptimizationEnabled(false);
    }

    @After
    public void tearDown() throws InterruptedException {
        TestUtils.shutdownAll(server);
    }

    @Test
    public void exceptionUnwrapping() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        doThrow(new MyRuntimeException()).when(testServiceMock).noArgs();

        Throwable t = null;
        try {
            proxy.noArgs();
            fail();
        } catch (MyRuntimeException expected) {
            t = expected;

            System.err.println("---------------------------------------------------");
            expected.printStackTrace();
            System.err.println("---------------------------------------------------");
        }

        assertContainsText(TestUtils.toString(t), "------End remote and begin local stracktrace ------");
    }

    @Test
    public void whenCalledWithNonNullArgument() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";

        proxy.singleArg(arg);

        verify(testServiceMock, times(1)).singleArg(eq(arg));
    }

    @Test
    public void whenCalledWithNullArgument() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";

        proxy.multipleArgs(arg, null);

        verify(testServiceMock, times(1)).multipleArgs(eq(arg), eq((String) null));
    }

    @Test
    public void whenNoArguments() {
        TestService proxy = proxyProvider.getProxy(TestService.class);

        proxy.noArgs();

        verify(testServiceMock, times(1)).noArgs();
    }

    @Test
    public void whenPrimitiveReturn() {
        TestService testService = proxyProvider.getProxy(TestService.class);
        int expected = 10;

        when(testServiceMock.primitiveReturn()).thenReturn(expected);

        int result = testService.primitiveReturn();
        assertEquals(expected, result);
    }

    @Test
    public void whenNullReturn() {
        TestService testService = proxyProvider.getProxy(TestService.class);
        when(testServiceMock.objectReturn()).thenReturn(null);

        Object result = testService.objectReturn();
        assertNull(result);
    }

    @Test
    public void whenObjectReturn() {
        TestService testService = proxyProvider.getProxy(TestService.class);
        String expected = "Foo";
        when(testServiceMock.objectReturn()).thenReturn(expected);

        Object result = testService.objectReturn();
        assertEquals(expected, result);
    }

    static public class Pojo implements HazelcastInstanceProvider {
        @Exposed
        public TestService testService;

        public final HazelcastInstance hazelcastInstance;

        public Pojo(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        public HazelcastInstance getHazelcastInstance() {
            return hazelcastInstance;
        }
    }

    static class MyRuntimeException extends RuntimeException {
        MyRuntimeException() {
        }
    }

    @DistributedService
    interface TestService {

        @LoadBalanced
        void noArgs();

        @LoadBalanced
        void singleArg(String arg);

        @LoadBalanced
        void multipleArgs(String arg, String arg2);

        @LoadBalanced
        Object objectReturn();

        @LoadBalanced
        int primitiveReturn();
    }
}
