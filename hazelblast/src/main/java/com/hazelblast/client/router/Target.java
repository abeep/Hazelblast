package com.hazelblast.client.router;

import com.hazelcast.core.Member;

/**
 * The result of a call to {@link Router#getTarget(java.lang.reflect.Method, Object[])}.
 *
 * @author Peter Veentjer.
 */
public class Target {

    private final Member member;
    private final int partitionId;

    public Target(Member member) {
        this(member, -1);
    }

    public Target(Member member, int partitionId) {
        this.member = member;
        this.partitionId = partitionId;
    }

    public Member getMember() {
        return member;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
