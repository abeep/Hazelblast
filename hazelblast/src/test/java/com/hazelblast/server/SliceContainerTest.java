package com.hazelblast.server;

import com.hazelblast.server.pojoslice.PojoSliceFactory;
import com.hazelcast.core.Hazelcast;
import org.junit.After;
import org.junit.Before;

public class SliceContainerTest {

    @Before
    public void setUp() {
        System.setProperty("puFactory.class", PojoSliceFactory.class.getName());
        System.setProperty("pojoPu.class", TestPojo.class.getName());
    }

    // ================= partitions ================


    @After
    public void tearDown(){
        Hazelcast.shutdownAll();
    }

    /*
    @Test
    public void containsPartition_whenNonExisting() {
        SliceContainer container = new SliceContainer(new PojoSlice(new TestPojo()),"default");
        boolean result = container.containsPartition(1);

        assertFalse(result);
    }

    @Test
    public void whenNewPartitionAdded() {
        SliceContainer container = new SliceContainer(new PojoSlice(new TestPojo()),"default");

        container.onPartitionAdded(1);

        boolean result = container.containsPartition(1);
        assertTrue(result);
        assertEquals(1, container.getPartitionCount());
    }

    @Test
    public void whenExistingPartitionAddedAgain() {
        SliceContainer container = new SliceContainer(new PojoSlice(new TestPojo()),"default");

        container.onPartitionAdded(1);
        try {
            container.onPartitionAdded(1);
            fail();
        } catch (IllegalArgumentException e) {

        }

        boolean result = container.containsPartition(1);
        assertTrue(result);
        assertEquals(1, container.getPartitionCount());
    }

    @Test
    public void whenExistingPartitionRemoved() {
        SliceContainer container = new SliceContainer(new PojoSlice(new TestPojo()),"default");

        container.onPartitionAdded(1);
        container.onPartitionRemoved(1);

        boolean result = container.containsPartition(1);
        assertFalse(result);
        assertEquals(0, container.getPartitionCount());
    }

    @Test
    public void whenNonExistingPartitionRemoved() {
        SliceContainer container = new SliceContainer(new PojoSlice(new TestPojo()),"default");

        try {
            container.onPartitionRemoved(1);
            fail();
        } catch (IllegalArgumentException e) {

        }

        boolean result = container.containsPartition(1);
        assertFalse(result);
        assertEquals(0, container.getPartitionCount());
    }

    // ================= partitions ================
     */
}
