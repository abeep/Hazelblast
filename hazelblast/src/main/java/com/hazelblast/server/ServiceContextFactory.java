package com.hazelblast.server;

/**
 * A Factory responsible for creating {@link com.hazelblast.server.ServiceContext} instances.
 * <p/>
 * By providing different ServiceContextFactory implementations, the system can switch between different mechanisms
 * (e.g. Spring or Google-Guice) to create a ServiceContext and set up the services to be exposed to the outside
 * world.
 *
 * @author Peter Veentjer.
 */
public interface ServiceContextFactory {

    /**
     * Creates a ServiceContext
     *
     * @return the created ServiceContext.
     */
    ServiceContext create();
}
