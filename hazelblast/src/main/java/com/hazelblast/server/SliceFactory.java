package com.hazelblast.server;

/**
 * A Factory responsible for creating {@link Slice} instances.
 * <p/>
 * By providing different SliceFactory implementations, the system can switch between different mechanisms
 * (e.g. Spring or Google-Guice) to create a Slice and set up the services to be exposed to the outside
 * world.
 * <p/>
 * A {@link SliceFactory} implementation needs to have at least a constructor with a single argument of
 * type {@link SliceConfig}. This method will be used to instantiate the SliceFactory.
 *
 * @author Peter Veentjer.
 */
public interface SliceFactory {

    /**
     * Creates a Slice
     *
     * @return the created Slice.
     */
    Slice create(SliceConfig sliceConfig);
}
