package com.hazelblast.client;

import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.PartitionKey;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.smarter.SmarterProxyProvider;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionAware;
import org.junit.*;

import static org.junit.Assert.*;

public class Partitioned_DefaultProxyProviderTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void setUp() {
        hazelcastInstance = Hazelcast.newHazelcastInstance(null);
    }

    @AfterClass
    public static void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_partitionedMethodWithoutArguments() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        proxyProvider.getProxy(PartitionedMethodWithoutArguments.class);
    }

    @DistributedService
    interface PartitionedMethodWithoutArguments {
        @Partitioned
        void method();
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_partitionedMethodWithoutPartitionKeyArgument() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        proxyProvider.getProxy(PartitionedMethodWithoutPartitionKeyArgument.class);
    }

    @DistributedService
    interface PartitionedMethodWithoutPartitionKeyArgument {
        @Partitioned
        void method(int arg1);
    }

    @Test
    @Ignore
    public void badProxy_partitionedMethodWithMultiplePartitionKeyArguments() {
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_partitionedMethodWithoutExistingProperty() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        proxyProvider.getProxy(PartitionedMethodWithoutExistingProperty.class);
    }

    @DistributedService
    interface PartitionedMethodWithoutExistingProperty {
        @Partitioned
        void method(@PartitionKey(property = "nonexising") String s);
    }

    @Test
    public void partitioned_whenPartitionAwareObjectReturnsNull() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        try {
            PartitionAwareObject p = new PartitionAwareObject(null);
            service.valid(p);
            fail();
        } catch (NullPointerException e) {

        }
    }

    @Test
    public void whenPartitionKeyWithPropertyIsNull() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        try {
            service.validWithProperty(null);
            fail();
        } catch (NullPointerException e) {

        }
    }

    @Test
    public void whenPartitionKeyArgumentWithPropertyReturnsNull() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        try {
            Person p = new Person(null);
            service.validWithProperty(p);
            fail();
        } catch (NullPointerException e) {

        }
    }

    @Test
    public void whenNullPartitionedKeyArgument() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);
        try {
            service.valid(null);
            fail();
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void normalPartitionKey() {
        StubExecutorService executorService = new StubExecutorService();
        executorService.result = "";
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider("default", hazelcastInstance, executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String arg = "foo";
        service.valid(arg);

        SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation x = (SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation) executorService.callable;
        assertEquals(arg, x.getPartitionKey());
    }

    @Test
    public void partitionKeyWithProperty() {
        StubExecutorService executorService = new StubExecutorService();
        executorService.result = "";
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider("default", hazelcastInstance, executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String name = "peter";
        Person person = new Person(name);
        service.validWithProperty(person);

        SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation x = (SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation) executorService.callable;
        assertEquals(name, x.getPartitionKey());
    }

    @Test
    public void partitionKeyWithPartitionAware() {
        StubExecutorService executorService = new StubExecutorService();
        executorService.result = "";
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider("default", hazelcastInstance, executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String partitionKey = "peter";
        PartitionAwareObject person = new PartitionAwareObject(partitionKey);
        service.valid(person);

        SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation x = (SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation) executorService.callable;
        assertEquals(partitionKey, x.getPartitionKey());
    }

    @DistributedService
    public interface PartitionedService {
        @Partitioned
        void valid(@PartitionKey Object a);

        @Partitioned
        void validWithProperty(@PartitionKey(property = "name") Person a);
    }

    public static class Person {
        final String name;

        Person(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }
    }

    static class PartitionAwareObject implements PartitionAware {
        private final Object partitionKey;

        PartitionAwareObject(Object partitionKey) {
            this.partitionKey = partitionKey;
        }

        public Object getPartitionKey() {
            return partitionKey;
        }
    }

    @Test
    public void test_toString() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);
        String s = service.toString();
        System.out.println(s);
        assertNotNull(s);
    }

    @Test
    public void test_hashCode() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);
        int s = service.hashCode();

    }

    @Test
    public void test_equals() {
        SmarterProxyProvider proxyProvider = new SmarterProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        assertTrue(service.equals(service));
        assertFalse(service.equals("foo"));
    }
}
