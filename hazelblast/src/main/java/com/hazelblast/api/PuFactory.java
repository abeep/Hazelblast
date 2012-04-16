package com.hazelblast.api;

/**
 * A Factory responsible for creating {@link ProcessingUnit} instances.
 * <p/>
 * By providing different PuFactory implementations, the system can switch between different mechanisms
 * (e.g. Spring or Guice) to create a ProcessingUnit and set up the services to be exposed to the outside
 * world.
 *
 * @author Peter Veentjer.
 */
public interface PuFactory {

    /**
     * Creates a ProcessingUnit
     *
     * @return the created ProcessingUnit.
     */
    ProcessingUnit create();
}
