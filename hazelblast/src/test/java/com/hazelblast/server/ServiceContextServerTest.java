package com.hazelblast.server;

import com.hazelblast.server.pojo.PojoServiceContextFactory;
import com.hazelcast.config.Config;
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

public class ServiceContextServerTest {

    private ServiceContextServer server;
    private ServiceContext serviceContextMock;

    @Before
    public void setUp() {
        System.setProperty("puFactory.class", PojoServiceContextFactory.class.getName());
        System.setProperty("pojoPu.class", TestPojo.class.getName());
        serviceContextMock = mock(ServiceContext.class);
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(null);
        server = new ServiceContextServer(serviceContextMock, "default", 1000, hazelcast);
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
        assertEquals(server.getStatus(), ServiceContextServer.Status.Unstarted);

        Mockito.verifyZeroInteractions(serviceContextMock);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
    }

    @Test
    public void start_whenUnstarted_thenStarted() {
        server.start();

        assertEquals(server.getStatus(), ServiceContextServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
        verify(serviceContextMock, times(1)).onStart();
        verify(serviceContextMock, times(0)).onStop();

    }

    @Test
    public void start_whenStarted_thenIgnored() {
        server.start();

        server.start();
        assertEquals(server.getStatus(), ServiceContextServer.Status.Running);
        assertFalse(server.isShutdown());
        assertFalse(server.isTerminated());
        assertFalse(server.isTerminating());
        verify(serviceContextMock, times(1)).onStart();
        verify(serviceContextMock, times(0)).onStop();
    }

    @Test
    public void start_whenTerminated_thenIllegalStateException() throws InterruptedException {
        server.start();
        server.shutdown();
        server.awaitTermination(10, TimeUnit.HOURS);

        reset(serviceContextMock);

        try {
            server.start();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(server.getStatus(), ServiceContextServer.Status.Terminated);
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        assertFalse(server.isTerminating());
        verifyZeroInteractions(serviceContextMock);
    }

    // =========================== shutdown =================================

    @Test
    public void shutdown_whenUnstarted() {
        server.shutdown();

        assertEquals(ServiceContextServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        verifyZeroInteractions(serviceContextMock);
    }

    @Test
    public void shutdown_whenRunning() throws InterruptedException {
        server.start();

        server.shutdown();
        server.awaitTermination();

        assertEquals(ServiceContextServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());

        Thread.sleep(1000);

        verify(serviceContextMock, times(1)).onStart();
        verify(serviceContextMock, times(1)).onStop();
    }

    @Test
    @Ignore
    public void shutdown_whenTerminating() {

    }

    @Test
    public void shutdown_whenTerminated() {
        server.shutdown();

        server.shutdown();

        assertEquals(ServiceContextServer.Status.Terminated, server.getStatus());
        assertTrue(server.isShutdown());
        assertTrue(server.isTerminated());
        assertFalse(server.isTerminating());
        verifyZeroInteractions(serviceContextMock);
    }
}
