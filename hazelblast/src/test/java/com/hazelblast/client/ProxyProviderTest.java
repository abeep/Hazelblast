package com.hazelblast.client;

import com.hazelblast.api.PartitionKey;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RemoteInterface;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.PartitionAware;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ProxyProviderTest {

    @Test(expected = NullPointerException.class)
    public void constructor_whenNullPuName() {
        new ProxyProvider(null, Hazelcast.getExecutorService());
    }

    @Test(expected = NullPointerException.class)
    public void constructor_whenNullExecutorService() {
        new ProxyProvider("foo", null);
    }

    @Test(expected = NullPointerException.class)
    public void getProxy_whenNull() {
        ProxyProvider proxyProvider = new ProxyProvider();
        proxyProvider.getProxy(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getProxy_whenNotInterface() {
        ProxyProvider proxyProvider = new ProxyProvider();
        proxyProvider.getProxy(String.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getProxy_whenNotRemoteInterface() {
        ProxyProvider proxyProvider = new ProxyProvider();
        proxyProvider.getProxy(List.class);
    }

    @Test
    public void getProxy_whenSuccess() {
        ProxyProvider proxyProvider = new ProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);
        assertNotNull(service);
    }

    @Test
    public void whenSameProxyAskedMultipleTimes_thenSameInstanceIsReturned() {
        ProxyProvider proxyProvider = new ProxyProvider();
        DummyRemoteService service1 = proxyProvider.getProxy(DummyRemoteService.class);
        DummyRemoteService service2 = proxyProvider.getProxy(DummyRemoteService.class);
        assertSame(service1, service2);
    }

    @Test
    public void partitioned_whenNoPartitionKeyIsFound() {
        ProxyProvider proxyProvider = new ProxyProvider();
        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        //the method signature doesn't contain any arguments.
        try {
            service.noArgument();
            fail();
        } catch (IllegalArgumentException e) {
        }

        //the method signature doesn't contain a @PartitionKey argument
        try {
            service.noPartitionKey("");
            fail();
        } catch (IllegalArgumentException e) {
        }

        //null argument
        try {
            service.valid(null);
            fail();
        } catch (NullPointerException e) {
        }

        //missing property
        try {
            service.validWithProperty("");
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void partitioned_normalPartitionKey() {
        DummyExecutorService executorService = new DummyExecutorService();
        executorService.result = "";
        ProxyProvider proxyProvider = new ProxyProvider("default", executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String arg = "foo";
        service.valid(arg);

        ProxyProvider.PartitionedMethodInvocation x = (ProxyProvider.PartitionedMethodInvocation) executorService.callable;
        assertEquals(arg, x.getPartitionKey());
    }

    @Test
    public void partitioned_partitionKeyWithProperty() {
        DummyExecutorService executorService = new DummyExecutorService();
        executorService.result = "";
        ProxyProvider proxyProvider = new ProxyProvider("default", executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String name = "peter";
        Person person = new Person(name);
        service.validWithProperty(person);

        ProxyProvider.PartitionedMethodInvocation x = (ProxyProvider.PartitionedMethodInvocation) executorService.callable;
        assertEquals(name, x.getPartitionKey());
    }

    @Test
    public void partitioned_partitionKeyWithPartitionAware() {
        DummyExecutorService executorService = new DummyExecutorService();
        executorService.result = "";
        ProxyProvider proxyProvider = new ProxyProvider("default", executorService);

        PartitionedService service = proxyProvider.getProxy(PartitionedService.class);

        String partitionKey = "peter";
        PartitionAwareObject person = new PartitionAwareObject(partitionKey);
        service.valid(person);

        ProxyProvider.PartitionedMethodInvocation x = (ProxyProvider.PartitionedMethodInvocation) executorService.callable;
        assertEquals(partitionKey, x.getPartitionKey());
    }

    @RemoteInterface
    interface PartitionedService {
        @Partitioned
        void noArgument();

        @Partitioned
        void noPartitionKey(String a);

        @Partitioned
        void valid(@PartitionKey Object a);

        @Partitioned
        void validWithProperty(@PartitionKey(property = "name") Object a);
    }

    @RemoteInterface
    interface DummyRemoteService {

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
}
