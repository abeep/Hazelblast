package com.hazelblast.server;

import com.hazelblast.server.pojo.PojoServiceContext;
import com.hazelblast.server.pojo.PojoServiceContextFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ServiceContextContainerTest {

    @Before
    public void setUp() {
        System.setProperty("puFactory.class", PojoServiceContextFactory.class.getName());
        System.setProperty("pojoPu.class", TestPojo.class.getName());
    }

    // ================= partitions ================

    @Test
    public void containsPartition_whenNonExisting() {
        ServiceContextContainer container = new ServiceContextContainer(new PojoServiceContext(new TestPojo()),"default");
        boolean result = container.containsPartition(1);

        assertFalse(result);
    }

    @Test
    public void whenNewPartitionAdded() {
        ServiceContextContainer container = new ServiceContextContainer(new PojoServiceContext(new TestPojo()),"default");

        container.onPartitionAdded(1);

        boolean result = container.containsPartition(1);
        assertTrue(result);
        assertEquals(1, container.getPartitionCount());
    }

    @Test
    public void whenExistingPartitionAddedAgain() {
        ServiceContextContainer container = new ServiceContextContainer(new PojoServiceContext(new TestPojo()),"default");

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
        ServiceContextContainer container = new ServiceContextContainer(new PojoServiceContext(new TestPojo()),"default");

        container.onPartitionAdded(1);
        container.onPartitionRemoved(1);

        boolean result = container.containsPartition(1);
        assertFalse(result);
        assertEquals(0, container.getPartitionCount());
    }

    @Test
    public void whenNonExistingPartitionRemoved() {
        ServiceContextContainer container = new ServiceContextContainer(new PojoServiceContext(new TestPojo()),"default");

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

}
