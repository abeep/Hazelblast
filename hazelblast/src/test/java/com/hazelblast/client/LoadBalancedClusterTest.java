package com.hazelblast.client;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.annotations.RemoteInterface;
import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceParameters;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelblast.server.pojoslice.PojoSliceFactory;
import com.hazelblast.server.pojoslice.Service;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LoadBalancedClusterTest {

    @Before
    public void before() {
        Hazelcast.shutdownAll();
    }

    @After
    public void after() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void test() throws Throwable {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(null);

        PojoSliceFactory factory = new PojoSliceFactory(Pojo.class);

        PojoSlice slice1 = factory.create(new SliceParameters(instance1));
        PojoSlice slice2 = factory.create(new SliceParameters(instance2));
        PojoSlice slice3 = factory.create(new SliceParameters(instance3));

        SomeServiceImpl service1 = (SomeServiceImpl) slice1.getService("someService");
        SomeServiceImpl service2 = (SomeServiceImpl) slice2.getService("someService");
        SomeServiceImpl service3 = (SomeServiceImpl) slice3.getService("someService");

        SliceServer server1 = build(slice1);
        SliceServer server2 = build(slice2);
        SliceServer server3 = build(slice3);

        HazelcastInstance clientInstance = TestUtils.newLiteInstance();

        ProxyProvider proxyProvider = new DefaultProxyProvider(clientInstance);
        SomeService someService = proxyProvider.getProxy(SomeService.class);

        for(int k=0;k<3*5;k++){
            someService.someMethod();
        }

        assertEquals(5, service1.count);
        assertEquals(5, service2.count);
        assertEquals(5, service3.count);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
    }

    public SliceServer build(Slice slice) {
        SliceServer server = new SliceServer(slice,1000);
        server.start();
        return server;
    }

    public static class Pojo {
        @Service
        public SomeService someService = new SomeServiceImpl();

        public Pojo() {
        }
    }

    @RemoteInterface
    public static interface SomeService {
        @LoadBalanced
        void someMethod();
    }

    public static class SomeServiceImpl implements SomeService{
        public int count;


        public void someMethod() {
            count++;
        }
    }
}
