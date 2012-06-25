package com.hazelblast.client;

import com.hazelcast.core.HazelcastInstance;

/**
 * Provides a 'client' side proxy to 'server' side implementations.
 * <p/>
 * A Slice is registered in the SliceServer with a given name (defaults to 'default'). So on a single
 * JVM multiple Slices can run in parallel. By providing a sliceName in this ProxyProvider, you can
 * control which Slice on the server side is going to be called.
 * <p/>
 * It is best to create a single instance of the ProxyProvider and to reuse it. The ProxyProvider implementations
 * are required to be thread-safe.
 * <p/>
 * Because the proxy is cached, the guarantee is given that always the same instance for a given interface is returned.
 *
 * @author Peter Veentjer.
 */
public interface ProxyProvider {

    /**
     * Returns the name of the {@link com.hazelblast.server.Slice} this ProxyProvider will send requests to.
     *
     * @return the name of the Slice.
     */
    String getSliceName();

    /**
     * Returns the (client-side) HazelcastInstance used by this ProxyProvider.
     *
     * @return the HazelcastInstance.
     */
    HazelcastInstance getHazelcastInstance();

    /**
     * Gets a proxy to to given interface.
     *
     * @param targetInterface the interface to connect to.
     * @param <T>
     * @return the created proxy.
     * @throws NullPointerException     if targetInterface is null.
     * @throws IllegalArgumentException if targetInterface is not an 'interface'.
     */
    <T> T getProxy(Class<T> targetInterface);
}
