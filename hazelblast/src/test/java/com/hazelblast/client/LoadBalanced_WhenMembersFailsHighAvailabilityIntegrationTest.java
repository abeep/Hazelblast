package com.hazelblast.client;

import com.hazelblast.TestUtils;
import com.hazelblast.client.annotations.LoadBalanced;
import com.hazelblast.client.annotations.RemoteInterface;
import com.hazelblast.client.loadbalancers.LoadBalancer;
import com.hazelblast.server.Slice;
import com.hazelblast.server.SliceParameters;
import com.hazelblast.server.SliceServer;
import com.hazelblast.server.pojoslice.PojoSlice;
import com.hazelblast.server.pojoslice.PojoSliceFactory;
import com.hazelblast.server.pojoslice.Service;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

public class LoadBalanced_WhenMembersFailsHighAvailabilityIntegrationTest {

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

        SliceServer server1 = new SliceServer(slice1,1000).start();
        SliceServer server2 = new SliceServer(slice2,1000).start();
        SliceServer server3 = new SliceServer(slice3,1000).start();

        HazelcastInstance clientInstance = TestUtils.newLiteInstance();

        ProxyProvider proxyProvider = new DefaultProxyProvider(clientInstance);
        SomeService someService = proxyProvider.getProxy(SomeService.class);

        Thread.sleep(10000);

        for (int k = 0; k < 100; k++) {
            System.out.println(k);
            someService.someMethod();
        }

        instance2.getLifecycleService().shutdown();

        for (int k = 0; k < 100; k++) {
            someService.someMethod();
        }

        instance1.getLifecycleService().shutdown();

        for (int k = 0; k < 100; k++) {
            someService.someMethod();
        }

        int sum = service1.count + service2.count + service3.count;
        assertEquals(300, sum);

        server1.shutdown();
        server2.shutdown();
        server3.shutdown();
    }

    public static class Pojo {
        @Service
        public SomeService someService = new SomeServiceImpl();

        public Pojo() {
        }
    }

    @RemoteInterface
    public static interface SomeService {

        @LoadBalanced(loadBalancer = TestLoadBalancer.class)
        void someMethod();
    }

    public static class SomeServiceImpl implements SomeService {
        public int count;


        public void someMethod() {
            count++;
        }
    }

    static class TestLoadBalancer implements LoadBalancer {

        private LinkedList<Member> members;
        private Iterator<Member> it;

        public TestLoadBalancer(HazelcastInstance hazelcastInstance) {
            this.members = new LinkedList();
            for (Member m : hazelcastInstance.getCluster().getMembers()) {
                if (!m.isLiteMember()) {
                    members.add(m);
                }
            }
            this.it = members.iterator();
        }

        public Member getNext() {
            Member next;
            if (it.hasNext()) {
                next = it.next();
            } else {
                it = members.iterator();
                next = it.next();
            }
            return next;

        }
    }
}
