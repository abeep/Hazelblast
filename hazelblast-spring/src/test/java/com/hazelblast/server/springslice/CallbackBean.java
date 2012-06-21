package com.hazelblast.server.springslice;

import com.hazelblast.server.SliceLifecycleListener;
import com.hazelblast.server.SlicePartitionListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.partition.Partition;

import java.util.concurrent.atomic.AtomicInteger;

public class CallbackBean implements SlicePartitionListener, SliceLifecycleListener, HazelcastInstanceAware {

    public final AtomicInteger onStartCount = new AtomicInteger();
    public final AtomicInteger onStopCount = new AtomicInteger();
    public Partition partitionAdded;
    public Partition partitionRemoved;
    public HazelcastInstance hazelcastInstance;

    public void onStart() {
        onStartCount.incrementAndGet();
    }

    public void onStop() {
        onStopCount.incrementAndGet();
    }

    public void onPartitionAdded(Partition partition) {
        partitionAdded = partition;
    }

    public void onPartitionRemoved(Partition partition) {
        partitionRemoved = partition;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }
}
