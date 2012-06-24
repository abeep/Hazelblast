package com.hazelblast.client.router;

import com.hazelcast.core.Member;

public class Target {

    private final Member member;
    private final long partitionId;

    public Target(Member member){
        this.member =member;
        partitionId = Long.MIN_VALUE;
    }

    public Target(Member member, long partitionId) {
        this.member = member;
        this.partitionId = partitionId;
    }

    public Member getMember() {
        return member;
    }

    public long getPartitionId() {
        return partitionId;
    }
}
