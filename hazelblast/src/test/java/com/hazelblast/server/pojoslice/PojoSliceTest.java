package com.hazelblast.server.pojoslice;

import com.hazelblast.server.SliceLifecycleAware;
import com.hazelblast.server.SlicePartitionAware;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.*;

public class PojoSliceTest {

    @Test
    public void partition_whenTargetNotImplementsPartitionAware() {
        List pojo = mock(List.class);
        PojoSlice slice = new PojoSlice(pojo);
        slice.onPartitionAdded(1);
        slice.onPartitionRemoved(2);

        verifyZeroInteractions(pojo);
    }

    @Test
    public void partition_whenTargetImplementsPartitionAware() {
        SlicePartitionAware pojo = mock(SlicePartitionAware.class);
        PojoSlice slice = new PojoSlice(pojo);
        slice.onPartitionAdded(1);
        slice.onPartitionRemoved(2);

        verify(pojo).onPartitionAdded(1);
        verify(pojo).onPartitionRemoved(2);
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
        SliceLifecycleAware pojo = mock(SliceLifecycleAware.class);
        PojoSlice slice = new PojoSlice(pojo);
        slice.onStart();
        slice.onStop();
        slice.onStop();

        verify(pojo).onStart();
        verify(pojo,times(2)).onStop();
        verifyNoMoreInteractions(pojo);
    }

}
