package com.hazelblast.server;

import com.hazelblast.server.pojoslice.PojoSliceFactory;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

public class SliceServerTest {

    private SliceServer server;
    private Slice sliceMock;

    @Before
    public void setUp() {
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(null);
        sliceMock = mock(Slice.class);
        when(sliceMock.getName()).thenReturn(Slice.DEFAULT_NAME);
        when(sliceMock.getHazelcastInstance()).thenReturn(hazelcast);
        server = new SliceServer(sliceMock,1000);
    }

    @After
    public void tearDown() throws InterruptedException {
        if (server != null) {
            server.shutdown();
            boolean terminated = server.awaitTermination(20, TimeUnit.SECONDS);
            assertTrue("Could not terminate the service within the given timeout", terminated);
        }

        Hazelcast.shutdownAll();
    }

    // =========================== start =================================


    @Test
    public void unstartedServer() {
        assertEquals(server.getStatus(), SliceServer.Status.Unstarted);

        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenUnstarted_thenStarted() {
        server.start();

        assertEquals(server.getStatus(), SliceServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
        verify(sliceMock, times(1)).onStart();
        verify(sliceMock, times(0)).onStop();

    }

    @Test
    public void start_whenStarted_thenIgnored() {
        server.start();

        server.start();
        assertEquals(server.getStatus(), SliceServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
        verify(sliceMock, times(1)).onStart();
        verify(sliceMock, times(0)).onStop();
    }

    @Test
    public void start_whenTerminated_thenIllegalStateException() throws InterruptedException {
        server.start();
        server.shutdown();
        server.awaitTermination(10, TimeUnit.HOURS);

        reset(sliceMock);

        try {
            server.start();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(server.getStatus(), SliceServer.Status.Terminated);
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    // =========================== shutdown =================================

    @Test
    public void shutdown_whenUnstarted() {
        server.shutdown();

        assertEquals(SliceServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
    }

    @Test
    public void shutdown_whenRunning() throws InterruptedException {
        server.start();

        server.shutdown();
        server.awaitTermination();

        assertEquals(SliceServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());

        Thread.sleep(1000);

        verify(sliceMock, times(1)).onStart();
        verify(sliceMock, times(1)).onStop();
    }

    @Test
    @Ignore
    public void shutdown_whenTerminating() {

    }

    @Test
    public void shutdown_whenTerminated() {
        server.shutdown();

        server.shutdown();

        assertEquals(SliceServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        assertFalse(server.isTerminating());
    }
}
