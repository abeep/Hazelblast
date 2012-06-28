package com.hazelblast.client.impl;


import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.Exposed;
import com.hazelblast.server.pojoslice.HazelcastInstanceProvider;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.*;

import static com.hazelblast.TestUtils.assertContainsText;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Partitioned_IntegrationTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();

        hazelcastInstance = Hazelcast.newHazelcastInstance(null);
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    private ProxyProviderImpl proxyProvider;
    private SliceServer server;
    private TestService testServiceMock;

    @After
    public void tearDown() throws InterruptedException {
        TestUtils.shutdownAll(server);
    }

    @Before
    public void setUp() throws InterruptedException {
         testServiceMock = mock(TestService.class);

        Pojo pojo = new Pojo(hazelcastInstance);
        pojo.testService = testServiceMock;
        PojoSlice slice = new PojoSlice(pojo);
        server = new SliceServer(slice, 100).start();

        proxyProvider = new ProxyProviderImpl("default", hazelcastInstance);
        proxyProvider.setLocalCallOptimizationEnabled(false);
    }

    static public class Pojo implements HazelcastInstanceProvider {

        @Exposed
        public TestService testService;

        private final HazelcastInstance hazelcastInstance;

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
        @Partitioned
        String singleArg(@PartitionKey String arg);

        @Partitioned
        String multipleArgs(@PartitionKey String arg, String arg2);
    }

    // ==================================== tests =================================================

    @Test
    public void exceptionUnwrapping() {
        TestService proxy = proxyProvider.getProxy(TestService.class);
        String arg = "foo";
        when(testServiceMock.singleArg(arg)).thenThrow(new MyRuntimeException());

        Throwable t = null;
        try {
            proxy.singleArg(arg);
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
        String result = "result";

        when(testServiceMock.singleArg(eq(arg))).thenReturn(result);

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

}
