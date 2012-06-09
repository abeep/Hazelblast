package com.hazelblast.api;

import com.hazelcast.core.DistributedTask;

import java.util.Collection;

public interface Reducer {

    Collection reduce(DistributedTask distributedTask);
}
