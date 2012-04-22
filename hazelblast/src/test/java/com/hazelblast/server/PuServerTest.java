package com.hazelblast.server;

import com.hazelblast.server.pojo.PojoPuFactory;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PuServerTest {

    @Before
    public void setUp() {
        System.setProperty("puFactory.class", PojoPuFactory.class.getName());
        System.setProperty("pojoPu.class", TestPojo.class.getName());
    }

    // =========================== start =================================

    @Test
    public void unstartedServer() {
        PuServer server = new PuServer();

        assertEquals(server.getStatus(), PuServer.Status.Unstarted);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenUnstarted_thenStarted() {
        PuServer server = new PuServer();

        server.start();

        assertEquals(server.getStatus(), PuServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenStarted_thenIgnored() {
        PuServer server = new PuServer();
        server.start();

        server.start();
        assertEquals(server.getStatus(), PuServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenTerminated_thenIllegalStateException() {
        PuServer server = new PuServer();
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
        PuServer server = new PuServer();
        server.shutdown();

        Assert.assertEquals(PuServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
    }

    @Test
    public void shutdown_whenRunning() throws InterruptedException {
        PuServer server = new PuServer(100);
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
        PuServer server = new PuServer();
        server.shutdown();

        server.shutdown();

        Assert.assertEquals(PuServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        assertFalse(server.isTerminating());
    }
}
