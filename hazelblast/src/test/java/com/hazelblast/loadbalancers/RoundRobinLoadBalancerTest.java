package com.hazelblast.loadbalancers;

import com.hazelblast.TestUtils;
import com.hazelblast.api.LoadBalancer;
import com.hazelblast.api.exceptions.NoMemberAvailableException;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.junit.After;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class RoundRobinLoadBalancerTest {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test(expected = NoMemberAvailableException.class)
    public void noMembersInTheCluster() {
        HazelcastInstance hazelcastInstance = TestUtils.newLiteInstance();

        LoadBalancer lb = new RoundRobinLoadBalancer(hazelcastInstance);
        lb.getNext();
    }

    public void assertRoundRobin(LoadBalancer loadBalancer, HazelcastInstance... members) {
        List<Member> cluster = new LinkedList<Member>();
        for (HazelcastInstance instance : members) {
            cluster.add(instance.getCluster().getLocalMember());
        }

        //read out the pattern.
        List<Member> pattern = new LinkedList<Member>();
        for (int k = 0; k < members.length; k++) {
            Member member = loadBalancer.getNext();
            assertTrue(String.format("member %s is not found in cluster %s", member, cluster), cluster.contains(member));
            assertFalse(pattern.contains(member));
            pattern.add(member);
        }

        //verify that the pattern is followed.
        for (int k = 0; k < 100; k++) {
            for (int l = 0; l < members.length; l++) {
                assertEquals(pattern.get(l), loadBalancer.getNext());
            }
        }
    }

    public void awaitSize(RoundRobinLoadBalancer lb, int expected){
        for(int k=0;k<10;k++){
            if(lb.getMemberCount()==expected){
                return;
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                fail();
            }
        }

        fail(String.format("Timeout: The lb failed to upscale/downscale to size %s, current size %s", expected, lb.getMemberCount()));
    }

    private HazelcastInstance newNormalInstance() {
        return Hazelcast.newHazelcastInstance(new Config());
    }

    @Test
    public void singleMember() {
        HazelcastInstance instance = newNormalInstance();
        LoadBalancer loadBalancer = new RoundRobinLoadBalancer(instance);
        Member first = loadBalancer.getNext();
        Member second = loadBalancer.getNext();
        Member self = instance.getCluster().getLocalMember();
        assertEquals(self, first);
        assertEquals(self, second);
    }

    @Test
    public void testMemberIsAdded() throws InterruptedException {
        HazelcastInstance instance1 = newNormalInstance();

        RoundRobinLoadBalancer loadBalancer = new RoundRobinLoadBalancer(instance1);
        assertRoundRobin(loadBalancer, instance1);

        //lets create an additional member
        HazelcastInstance instance2 = newNormalInstance();

        awaitSize(loadBalancer, 2);

        assertRoundRobin(loadBalancer, instance1, instance2);
    }

    @Test
    public void liteMemberIsAdded() {
        HazelcastInstance instance1 = newNormalInstance();

        LoadBalancer loadBalancer = new RoundRobinLoadBalancer(instance1);
        assertRoundRobin(loadBalancer, instance1);


        //lets create an additional member
        TestUtils.newLiteInstance();

        assertRoundRobin(loadBalancer, instance1);
    }

    @Test
    public void memberIsRemoved() throws InterruptedException {
        HazelcastInstance instance1 = newNormalInstance();
        HazelcastInstance instance2 = newNormalInstance();

        RoundRobinLoadBalancer loadBalancer = new RoundRobinLoadBalancer(instance1);

        assertRoundRobin(loadBalancer, instance1, instance2);

        //now remove a member
        instance2.getLifecycleService().shutdown();

        awaitSize(loadBalancer, 1);

        assertRoundRobin(loadBalancer, instance1);
    }

    @Test
    public void multipleMembers() throws InterruptedException {
        HazelcastInstance[] instances = new HazelcastInstance[5];
        for (int k = 0; k < instances.length; k++) {
            instances[k] = newNormalInstance();
        }

        LoadBalancer lb = new RoundRobinLoadBalancer(instances[0]);
        assertRoundRobin(lb, instances);
    }
}
