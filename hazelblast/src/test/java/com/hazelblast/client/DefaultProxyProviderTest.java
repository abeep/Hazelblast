package com.hazelblast.client;

import com.hazelblast.client.annotations.DistributedService;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.annotations.Partitioned;
import com.hazelblast.client.basic.BasicProxyProvider;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class DefaultProxyProviderTest {

    private static HazelcastInstance hazelcastInstance;

    @BeforeClass
    public static void beforeClass() {
        hazelcastInstance = Hazelcast.newHazelcastInstance(null);
    }

    @AfterClass
    public static void afterClass() {
        hazelcastInstance.getLifecycleService().shutdown();
    }

    @Test(expected = NullPointerException.class)
    public void constructor_whenNullSliceName() {
        new BasicProxyProvider(null, hazelcastInstance);
    }

    @Test(expected = NullPointerException.class)
    public void constructor_whenNullExecutorService() {
        new BasicProxyProvider("foo", null);
    }

    @Test(expected = NullPointerException.class)
    public void getProxy_whenNull() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        proxyProvider.getProxy(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getProxy_whenNotInterface() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        proxyProvider.getProxy(String.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getProxy_whenNotRemoteInterface() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        proxyProvider.getProxy(List.class);
    }

    @Test
    public void getProxy_whenSuccess() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);
        assertNotNull(service);
    }

    @Test
    public void whenSameProxyAskedMultipleTimes_thenSameInstanceIsReturned() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        DummyRemoteService service1 = proxyProvider.getProxy(DummyRemoteService.class);
        DummyRemoteService service2 = proxyProvider.getProxy(DummyRemoteService.class);
        assertSame(service1, service2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_remoteInterfaceExtendingNonRemoteInterface() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        proxyProvider.getProxy(RemoteInterfaceExtendingNonRemoteInterface.class);
    }

    @DistributedService
    public interface RemoteInterfaceExtendingNonRemoteInterface extends NonRemoteInterface {
    }

    interface NonRemoteInterface {
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_methodsWithMultipleAnnotations() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        proxyProvider.getProxy(MethodWithMultipleAnnotations.class);
    }

    @DistributedService
    interface MethodWithMultipleAnnotations {
        @Partitioned
        @LoadBalanced
        void method();
    }

    @Test(expected = IllegalArgumentException.class)
    public void badProxy_remoteMethodAnnotationMissing() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        proxyProvider.getProxy(RemoteAnnotationMissing.class);
    }

    @DistributedService
    interface RemoteAnnotationMissing {
        void method();
    }


    @DistributedService
    interface DummyRemoteService {
    }

    @Test
    public void test_toString() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);
        String s = service.toString();
        System.out.println(s);
        assertNotNull(s);
    }

    @Test
    public void test_hashCode() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);
        int s = service.hashCode();

    }

    @Test
    public void test_equals() {
        BasicProxyProvider proxyProvider = new BasicProxyProvider();
        DummyRemoteService service = proxyProvider.getProxy(DummyRemoteService.class);

        assertTrue(service.equals(service));
        assertFalse(service.equals("foo"));
    }
}
