package com.hazelblast.client.loadbalancers;

import com.hazelblast.server.exceptions.NoMemberAvailableException;
import com.hazelcast.core.*;
import com.hazelcast.logging.ILogger;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * A {@link ContentBasedLoadBalancer} that uses round robin to iterate over the members of the cluster.
 *
 * @author Peter Veentjer.
 */
public class RoundRobinLoadBalancer implements ContentBasedLoadBalancer {
    private final ILogger logger;

    private final AtomicInteger counter = new AtomicInteger();
    private final Cluster cluster;
    private final AtomicReference<List<Member>> members = new AtomicReference<List<Member>>();
    private final HazelcastInstance hazelcastInstance;

    public RoundRobinLoadBalancer(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = notNull("hazelcastInstance", hazelcastInstance);
        this.logger = hazelcastInstance.getLoggingService().getLogger(RoundRobinLoadBalancer.class.getName());
        this.cluster = hazelcastInstance.getCluster();
        this.cluster.addMembershipListener(new MembershipListenerImpl());
        reset();
    }

    public int getMemberCount() {
        return members.get().size();
    }

    public Member getNext(Method method, Object[] args) {
        List<Member> memberList = members.get();
        if (memberList.isEmpty()) {
            throw new NoMemberAvailableException(
                    format("RoundRobinLoadBalancer: There are no members in the cluster of Hazelcast instance [%s]",hazelcastInstance.getName()));
        }

        int count = counter.getAndIncrement();
        if (count < 0) {
            count = -count;
        }

        Member member = memberList.get(count % memberList.size());
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.INFO, format("Next request is send to member '%s'", member));
        }
        return member;
    }

    private void reset() {
        List<Member> memberList = new ArrayList<Member>();
        for (Member member : cluster.getMembers()) {
            if (!member.isLiteMember()) {
                memberList.add(member);
            }
        }

        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, format("The following members are part of the cluster %s", memberList));
        }

        members.set(memberList);
        counter.set(0);
    }

    private class MembershipListenerImpl implements MembershipListener {

        public void memberAdded(MembershipEvent e) {
            if (e.getMember().isLiteMember()) {
                return;
            }

            reset();
        }

        public void memberRemoved(MembershipEvent e) {
            if (e.getMember().isLiteMember()) {
                return;
            }

            reset();
        }
    }
}
