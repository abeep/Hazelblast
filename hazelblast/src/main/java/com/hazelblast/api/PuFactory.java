package com.hazelblast.api;

/**
 * A Factory responsible for creating {@link ProcessingUnit} instances.
 *
 * By providing different PuFactory implementations, the system can switch between different mechanisms
 * (e.g. Spring or Guice) to create a ProcessingUnit and set up the services to be exposed to the outside
 * world.
 */
public interface PuFactory {

    /**
     * Creates a ProcessingUnit
     *
     * @param partitionId the id of the partition the ProcessingUnit is created for.
     * @return the created ProcessingUnit.
     */
    ProcessingUnit create(int partitionId);
}
