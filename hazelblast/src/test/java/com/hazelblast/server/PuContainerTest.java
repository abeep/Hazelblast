package com.hazelblast.server;

import com.hazelblast.server.pojo.PojoPu;
import com.hazelblast.server.pojo.PojoPuFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PuContainerTest {

    @Before
    public void setUp() {
        System.setProperty("puFactory.class", PojoPuFactory.class.getName());
        System.setProperty("pojoPu.class", TestPojo.class.getName());
    }

    // ================= partitions ================

    @Test
    public void containsPartition_whenNonExisting() {
        PuContainer container = new PuContainer(new PojoPu(new TestPojo()),"default");
        boolean result = container.containsPartition(1);

        assertFalse(result);
    }

    @Test
    public void whenNewPartitionAdded() {
        PuContainer container = new PuContainer(new PojoPu(new TestPojo()),"default");

        container.onPartitionAdded(1);

        boolean result = container.containsPartition(1);
        assertTrue(result);
        assertEquals(1, container.getPartitionCount());
    }

    @Test
    public void whenExistingPartitionAddedAgain() {
        PuContainer container = new PuContainer(new PojoPu(new TestPojo()),"default");

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
        PuContainer container = new PuContainer(new PojoPu(new TestPojo()),"default");

        container.onPartitionAdded(1);
        container.onPartitionRemoved(1);

        boolean result = container.containsPartition(1);
        assertFalse(result);
        assertEquals(0, container.getPartitionCount());
    }

    @Test
    public void whenNonExistingPartitionRemoved() {
        PuContainer container = new PuContainer(new PojoPu(new TestPojo()),"default");

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
