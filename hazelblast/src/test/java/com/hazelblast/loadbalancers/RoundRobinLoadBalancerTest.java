package com.hazelblast.loadbalancers;

import com.hazelblast.api.LoadBalancer;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RoundRobinLoadBalancerTest {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void singleMember() {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        LoadBalancer loadBalancer = new RoundRobinLoadBalancer(instance);
        Member member1 = loadBalancer.findTargetMember();
        Member member2 = loadBalancer.findTargetMember();
        Member self = instance.getCluster().getLocalMember();
        assertEquals(self, member1);
        assertEquals(self, member2);
    }

    @Test
    @Ignore
    public void testNoMemberInTheCluster(){

    }

    @Test
    @Ignore
    public void testMemberIsAdded(){

    }

    @Test
    @Ignore
    public void liteMemberIsAdded(){

    }

    @Test
    @Ignore
    public void memberIsRemoved(){

    }

    @Test
    public void multipleMembers() {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(null);

        Member member1 = instance1.getCluster().getLocalMember();
        Member member2 = instance2.getCluster().getLocalMember();
        Member member3 = instance3.getCluster().getLocalMember();

        System.out.println("===============================================================");

        System.out.println("cluster member:" + instance3.getCluster().getMembers());

        LoadBalancer loadBalancer = new RoundRobinLoadBalancer(instance1);
        for(int k=0;k<10;k++){
            Member member = loadBalancer.findTargetMember();
            System.out.println(member);
        }
    }
}
