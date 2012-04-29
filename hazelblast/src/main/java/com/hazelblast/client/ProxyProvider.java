package com.hazelblast.client;

/**
 * Provides a 'client' side proxy to 'server' side implementations.
 * <p/>
 * A pu is registered in the PuServer with a given name (defaults to 'default'). So on a single JVM multiple processing
 * units can run in parallel. By providing a puName in this ProxyProvider, you can control which pu on the serverside
 * is going to be called.
 * <p/>
 * It is best to create a single instance of the ProxyProvider and to reuse it. The ProxyProvider is threadsafe.
 * <p/>
 * Because the proxy is cached, the guarantee is given that always the same instance for a given interface is returned.
 *
 * @author Peter Veentjer.
 */
public interface ProxyProvider {

    /**
     * Returns the name of the processing unit this ProxyProvider will send requests to.
     *
     * @return the name of the pu.
     */
    String getPuName();

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
