package com.hazelblast.api;

/**
 * The ProcessingUnit is an place where all kinds of Services can be exposed to the outside world.
 */
public interface ProcessingUnit {

    /**
     * Gets the Service with the given name.
     *
     * @param name the name of the service to look for.
     * @return the found service, or null if the Service doesn't exist.
     */
    Object getService(String name);

    /**
     * Starts this processing unit.
     * <p/>
     * This method will be called only once on an instance.
     */
    void start();

    /**
     * Stops this processing unit.
     * <p/>
     * This method will be called only once on an instance.
     */
    void stop();
}
