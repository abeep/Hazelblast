package com.hazelblast.server.pojoslice;

import com.hazelblast.server.SliceLifecycleListener;
import com.hazelblast.server.SlicePartitionListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.partition.Partition;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.*;

public class PojoSliceTest {
    @Test
    public void partition_whenTargetNotImplementsPartitionAware() {
        List pojo = mock(List.class);
        Partition partition1 = mock(Partition.class);
        Partition partition2 = mock(Partition.class);

        PojoSlice slice = new PojoSlice(pojo);
        slice.onPartitionAdded(partition1);
        slice.onPartitionRemoved(partition2);

        verifyZeroInteractions(pojo);
    }

    @Test
    public void partition_whenTargetImplementsPartitionAware() {
        Partition partition1 = mock(Partition.class);
        Partition partition2 = mock(Partition.class);


        SlicePartitionListener pojo = mock(SlicePartitionListener.class);
        PojoSlice slice = new PojoSlice(pojo);
        slice.onPartitionAdded(partition1);
        slice.onPartitionRemoved(partition2);

        verify(pojo).onPartitionAdded(partition1);
        verify(pojo).onPartitionRemoved(partition2);
        verifyNoMoreInteractions(pojo);
    }

    @Test
    public void lifecycle_whenTargetNotImplementsLifecycleAware() {
        List pojo = mock(List.class);
        PojoSlice slice = new PojoSlice(pojo);
        slice.onStart();
        slice.onStop();

        verifyZeroInteractions(pojo);
    }

    @Test
    public void lifecycle_whenTargetImplementsLifecycleAware() {
        SliceLifecycleListener pojo = mock(SliceLifecycleListener.class);
        PojoSlice slice = new PojoSlice(pojo);
        slice.onStart();
        slice.onStop();
        slice.onStop();

        verify(pojo).onStart();
        verify(pojo, times(2)).onStop();
        verifyNoMoreInteractions(pojo);
    }

    @Test
    public void instanceAware_whenTargetImplementsHazelcastInstanceAware() {
        HazelcastInstanceAware pojo = mock(HazelcastInstanceAware.class);
        HazelcastInstance hazelcastInstance = Hazelcast.getDefaultInstance();
        PojoSlice slice = new PojoSlice(pojo,hazelcastInstance);
        slice.onStart();

        verify(pojo).setHazelcastInstance(hazelcastInstance);
        verifyNoMoreInteractions(pojo);
    }

}
