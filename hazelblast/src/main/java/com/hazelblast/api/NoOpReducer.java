package com.hazelblast.api;

import com.hazelcast.core.DistributedTask;

import java.util.Collection;

public class NoOpReducer implements Reducer {

    public Collection reduce(DistributedTask distributedTask) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
