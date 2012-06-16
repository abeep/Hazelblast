package com.hazelblast.client;

import com.hazelblast.api.LoadBalanced;
import com.hazelblast.api.Partitioned;
import com.hazelblast.api.RemoteInterface;
import com.hazelblast.client.basic.DefaultProxyProvider;
import com.hazelcast.core.Hazelcast;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class DefaultProxyProviderTest {

    @Test(expected = NullPointerException.class)
    public void constructor_whenNullPuName() {
        new DefaultProxyProvider(null, Hazelcast.getDefaultInstance());
    }

    @Test(expected = NullPointerException.class)
    public void constructor_whenNullExecutorService() {
        new DefaultProxyProvider("foo", null);
    }

    @Test(expected = NullPointerException.class)
    public void getProxy_whenNull() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getProxy_whenNotInterface() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(String.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getProxy_whenNotRemoteInterface() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(List.class);
    }

    @Test
    public void getProxy_whenSuccess() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);
        assertNotNull(service);
    }

    @Test
    public void whenSameProxyAskedMultipleTimes_thenSameInstanceIsReturned() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        DummyRemoteService service1 = proxyProvider.getProxy(DummyRemoteService.class);
        DummyRemoteService service2 = proxyProvider.getProxy(DummyRemoteService.class);
        assertSame(service1, service2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_remoteInterfaceExtendingNonRemoteInterface() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(RemoteInterfaceExtendingNonRemoteInterface.class);
    }

    @RemoteInterface
    public interface RemoteInterfaceExtendingNonRemoteInterface extends NonRemoteInterface {
    }

    interface NonRemoteInterface {
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_methodsWithMultipleAnnotations() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(MethodWithMultipleAnnotations.class);
    }

    @RemoteInterface
    interface MethodWithMultipleAnnotations {
        @Partitioned
        @LoadBalanced
        void method();
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_remoteMethodAnnotationMissing() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        proxyProvider.getProxy(RemoteAnnotationMissing.class);
    }

    @RemoteInterface
    interface RemoteAnnotationMissing {
        void method();
    }


    @RemoteInterface
    interface DummyRemoteService {
    }

    @Test
    public void test_toString() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);
        String s = service.toString();
        System.out.println(s);
        assertNotNull(s);
    }

    @Test
    public void test_hashCode() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);
        int s = service.hashCode();

    }

    @Test
    public void test_equals() {
        DefaultProxyProvider proxyProvider = new DefaultProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);

        assertTrue(service.equals(service));
        assertFalse(service.equals("foo"));
    }
}
