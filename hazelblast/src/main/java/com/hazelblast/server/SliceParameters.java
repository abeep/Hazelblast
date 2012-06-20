package com.hazelblast.server;

import com.hazelcast.core.HazelcastInstance;

import static com.hazelblast.utils.Arguments.notNull;

public class SliceParameters {

    public final HazelcastInstance hazelcastInstance;
    public final String name;

    public SliceParameters(HazelcastInstance hazelcastInstance){
        this(hazelcastInstance,Slice.DEFAULT_NAME);
    }

    public SliceParameters(HazelcastInstance hazelcastInstance, String name) {
        this.hazelcastInstance = notNull("hazelcastInstance",hazelcastInstance);
        this.name = notNull("name",name);
    }
}
