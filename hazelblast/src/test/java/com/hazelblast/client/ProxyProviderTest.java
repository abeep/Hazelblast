package com.hazelblast.client;

import com.hazelblast.api.RemoteInterface;
import com.hazelcast.core.Hazelcast;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

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

    @RemoteInterface
    interface DummyRemoteService {

    }
}
