package com.hazelblast.client;

import com.hazelblast.api.PartitionKey;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RemoteInterface;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitionAware;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class Partitioned_DefaultProxyProviderTest {

    private HazelcastInstance hazelcastInstance;

    @Before
    public void setUp() {
        hazelcastInstance = Hazelcast.newHazelcastInstance(null);
    }

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_partitionedMethodWithoutArguments() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(PartitionedMethodWithoutArguments.class);
    }

    @RemoteInterface
    interface PartitionedMethodWithoutArguments {
        @Partitioned
        void method();
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_partitionedMethodWithoutPartitionKeyArgument() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(PartitionedMethodWithoutPartitionKeyArgument.class);

    }

    @RemoteInterface
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
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(PartitionedMethodWithoutExistingProperty.class);
    }

    @RemoteInterface
    interface PartitionedMethodWithoutExistingProperty {
        @Partitioned
        void method(@PartitionKey(property = "nonexising") String s);
    }

    @Test
    public void partitioned_whenPartitionAwareObjectReturnsNull() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        try {
            PartitionAwareObject p = new PartitionAwareObject(null);
            service.valid(p);
            fail();
        } catch (NullPointerException e) {

        }
    }

    @Test
    public void partitioned_whenPartitionKeyWithPropertyIsNull() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        try {
            service.validWithProperty(null);
            fail();
        } catch (NullPointerException e) {

        }
    }

    @Test
    public void partitioned_whenPartitionKeyArgumentWithPropertyReturnsNull() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        try {
            Person p = new Person(null);
            service.validWithProperty(p);
            fail();
        } catch (NullPointerException e) {

        }
    }

    @Test
    public void partitioned_whenNullPartitionedKeyArgument() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);
        try {
            service.valid(null);
            fail();
        } catch (NullPointerException e) {
        }
    }

    @Test
    public void partitioned_normalPartitionKey() {
        StubExecutorService executorService = new StubExecutorService();
        executorService.result = "";
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider("default", hazelcastInstance, executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String arg = "foo";
        service.valid(arg);

        SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation x = (SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation) executorService.callable;
        assertEquals(arg, x.getPartitionKey());
    }

    @Test
    public void partitioned_partitionKeyWithProperty() {
        StubExecutorService executorService = new StubExecutorService();
        executorService.result = "";
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider("default", hazelcastInstance, executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String name = "peter";
        Person person = new Person(name);
        service.validWithProperty(person);

        SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation x = (SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation) executorService.callable;
        assertEquals(name, x.getPartitionKey());
    }

    @Test
    public void partitioned_partitionKeyWithPartitionAware() {
        StubExecutorService executorService = new StubExecutorService();
        executorService.result = "";
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider("default", hazelcastInstance, executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String partitionKey = "peter";
        PartitionAwareObject person = new PartitionAwareObject(partitionKey);
        service.valid(person);

        SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation x = (SerializableRemoteMethodInvocationFactory.RemoteMethodInvocation) executorService.callable;
        assertEquals(partitionKey, x.getPartitionKey());
    }

    @RemoteInterface
    interface PartitionedService {
        @Partitioned
        void valid(@PartitionKey Object a);

        @Partitioned
        void validWithProperty(@PartitionKey(property = "name") Person a);
    }

    static class Person {
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
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);
        String s = service.toString();
        System.out.println(s);
        assertNotNull(s);
    }

    @Test
    public void test_hashCode() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);
        int s = service.hashCode();

    }

    @Test
    public void test_equals() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        assertTrue(service.equals(service));
        assertFalse(service.equals("foo"));
    }
}
