package com.hazelblast.loadbalancers;

import com.hazelblast.api.LoadBalancer;
import com.hazelblast.api.exceptions.NoMemberAvailableException;
import com.hazelcast.core.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelblast.utils.Arguments.notNull;

//todo: logging
public class RoundRobinLoadBalancer implements LoadBalancer {
    private final AtomicInteger counter = new AtomicInteger();
    private final Cluster cluster;
    private final AtomicReference<List<Member>> members = new AtomicReference<List<Member>>();

    public RoundRobinLoadBalancer(HazelcastInstance hazelcastInstance) {
        notNull("hazelcastInstance", hazelcastInstance);
        cluster = hazelcastInstance.getCluster();
        cluster.addMembershipListener(new MembershipListenerImpl());
        reset();
    }

    public int getMemberCount(){
        return members.get().size();
    }

    public Member getNext() {
        List<Member> memberList = members.get();
        if(memberList.isEmpty()){
            throw new NoMemberAvailableException("RoundRobinLoadBalancer: There are no members in the cluster");
        }

        int count = counter.getAndIncrement();
        if (count < 0) {
            count = -count;
        }
        return memberList.get(count % memberList.size());
    }

    private void reset() {
        List<Member> memberList = new ArrayList<Member>();
        for(Member member: cluster.getMembers()){
            if(!member.isLiteMember()){
                memberList.add(member);
            }
        }

        members.set(memberList);
        counter.set(0);
    }

    private class MembershipListenerImpl implements MembershipListener {

        public void memberAdded(MembershipEvent e) {
            if(e.getMember().isLiteMember()){
                return;
            }

            reset();
        }

        public void memberRemoved(MembershipEvent e) {
            if(e.getMember().isLiteMember()){
                return;
            }

            reset();
        }
    }
}
