package com.hazelblast.server.spring;

public interface SpringPartitionListener {

    void onPartitionAdded(int partitionId);

    void onPartitionRemoved(int partitionId);
}
