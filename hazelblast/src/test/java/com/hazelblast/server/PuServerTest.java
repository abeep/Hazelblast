package com.hazelblast.server;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.server.pojo.PojoPuFactory;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PuServerTest {

    private PuServer server;
    private ProcessingUnit puMock;

    @Before
    public void setUp() {
        System.setProperty("puFactory.class", PojoPuFactory.class.getName());
        System.setProperty("pojoPu.class", TestPojo.class.getName());
        puMock = createMock(ProcessingUnit.class);
        server = new PuServer(puMock,"default");
    }

    @After
    public void tearDown() {
        if (server != null) {
            server.shutdown();
        }
    }

    // =========================== start =================================


    @Test
    public void unstartedServer() {
        assertEquals(server.getStatus(), PuServer.Status.Unstarted);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenUnstarted_thenStarted() {
        server.start();

        assertEquals(server.getStatus(), PuServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenStarted_thenIgnored() {
        server.start();

        server.start();
        assertEquals(server.getStatus(), PuServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenTerminated_thenIllegalStateException() {
        server.shutdown();

        try {
            server.start();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(server.getStatus(), PuServer.Status.Terminated);
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        assertFalse(server.isTerminating());

    }

    // =========================== shutdown =================================

    @Test
    public void shutdown_whenUnstarted() {
        server.shutdown();

        Assert.assertEquals(PuServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
    }

    @Test
    public void shutdown_whenRunning() throws InterruptedException {
        server.start();

        server.shutdown();
        server.awaitTermination();

        Assert.assertEquals(PuServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
    }

    @Test
    @Ignore
    public void shutdown_whenTerminating() {

    }

    @Test
    public void shutdown_whenTerminated() {
        server.shutdown();

        server.shutdown();

        Assert.assertEquals(PuServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        assertFalse(server.isTerminating());
    }
}
