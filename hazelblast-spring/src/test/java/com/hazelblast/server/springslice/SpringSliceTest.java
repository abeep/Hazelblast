package com.hazelblast.server.springslice;

import com.hazelblast.server.SliceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.partition.Partition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class SpringSliceTest {

    @BeforeClass
    public static void beforeClass() {
        Hazelcast.shutdownAll();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    @Ignore
    public void whenBeansStuff() {

    }

    @Test
    public void whenLifecycleCallback() {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("callback.xml");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        SpringSlice slice = new SpringSlice(new SliceConfig(), applicationContext, instance);
        CallbackBean callbackBean = (CallbackBean) slice.getApplicationContext().getBean("callbackBean");

        Partition p1 = mock(Partition.class);
        Partition p2 = mock(Partition.class);

        slice.onStart();

        slice.onPartitionAdded(p1);
        slice.onPartitionRemoved(p2);
        assertSame(p1, callbackBean.partitionAdded);
        assertSame(p2, callbackBean.partitionRemoved);
    }

    @Test
    public void whenPartitionStuff() {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("callback.xml");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);
        SpringSlice slice = new SpringSlice(new SliceConfig(), applicationContext, instance);
        CallbackBean callbackBean = (CallbackBean) slice.getApplicationContext().getBean("callbackBean");

        slice.onStart();
        assertSame(instance, callbackBean.hazelcastInstance);
        assertEquals(1, callbackBean.onStartCount.get());
        assertEquals(0, callbackBean.onStopCount.get());

        slice.onStop();

        assertEquals(1, callbackBean.onStartCount.get());
        assertEquals(1, callbackBean.onStopCount.get());
    }

    @Test
    public void whenExplicitHazelcastInstanceDefined() {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("explicit-hazelcast-instance.xml");
        HazelcastInstance defaultInstance = Hazelcast.getDefaultInstance();

        SpringSlice slice = new SpringSlice(new SliceConfig(), applicationContext, defaultInstance);
        slice.onStart();

        assertNotNull(slice.getHazelcastInstance());
        assertNotSame(defaultInstance, slice.getHazelcastInstance());
    }

    @Test
    public void whenNoHazelcastInstanceDefinedThenUseDefault() {
        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("no-hazelcast-instance.xml");
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(null);

        SpringSlice slice = new SpringSlice(new SliceConfig(), applicationContext, instance);
        slice.onStart();

        assertSame(instance, slice.getHazelcastInstance());
    }
}
