package com.hazelblast.server;

import com.hazelblast.server.pojo.PojoServiceContext;
import com.hazelblast.server.pojo.PojoServiceContextFactory;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InMemoryClusterIntegrationTest {

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

        PojoServiceContextFactory factory = new PojoServiceContextFactory(Pojo.class);

        PojoServiceContext context1 = factory.create();
        PojoServiceContext context2 = factory.create();
        PojoServiceContext context3 = factory.create();

        SomeService service1 = (SomeService) context1.getService("someService");
        SomeService service2 = (SomeService) context2.getService("someService");
        SomeService service3 = (SomeService) context3.getService("someService");

        ServiceContextServer server1 = build(context1, instance1, "foo");
        ServiceContextServer server2 = build(context2, instance2, "foo");
        ServiceContextServer server3 = build(context3, instance3, "foo");

        ServiceContextServer.executeMethod(instance1, "foo", "SomeService", "someMethod", new String[]{}, new Object[]{}, null);
        ServiceContextServer.executeMethod(instance2, "foo", "SomeService", "someMethod", new String[]{}, new Object[]{}, null);
        ServiceContextServer.executeMethod(instance2, "foo", "SomeService", "someMethod", new String[]{}, new Object[]{}, null);

        assertEquals(1, service1.count);
        assertEquals(2, service2.count);
        assertEquals(0, service3.count);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
    }

    public ServiceContextServer build(ServiceContext context, HazelcastInstance hazelcastInstance, String name) {
        System.setProperty("puFactory.class", PojoServiceContextFactory.class.getName());
        System.setProperty("pojoPu.class", Pojo.class.getName());
        ServiceContextServer server = new ServiceContextServer(context, name, 1000, hazelcastInstance);
        server.start();
        return server;
    }

    public static class Pojo {
        public SomeService someService = new SomeService();

        public Pojo() {
        }
    }

    public static class SomeService {
        public int count;

        public void someMethod() {
            count++;
        }
    }
}
