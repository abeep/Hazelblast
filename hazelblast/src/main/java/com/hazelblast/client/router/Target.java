package com.hazelblast.client.router;

import com.hazelcast.core.Member;

public class Target {

    private final Member member;
    private final int partitionId;

    public Target(Member member){
        this.member =member;
        partitionId = -1;
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
