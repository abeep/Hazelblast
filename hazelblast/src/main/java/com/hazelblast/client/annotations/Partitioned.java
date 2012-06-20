package com.hazelblast.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that can be placed on a method to indicate that the call should be send to a specific partition.
 *
 * Each {@link Partitioned} method should have:
 * <ol>
 *     <li>at least 1 argument</li>
 *     <li>exactly 1 argument should have the {@link PartitionKey} annotation</li>
 * </ol>
 *
 * @author Peter Veentjer.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Partitioned {

    /**
     * The maximum time to wait for a call to complete.
     *
     * @return the maximum time to wait for a call to complete.
     */
    long timeoutMs() default 60 * 1000;

    /**
     * Optionally interrupts the thread that executed the call on the serverside when a timeout happens.
     *
     * @return if the serverside thread should be interrupted on timeout.
     */
    boolean interruptOnTimeout() default true;
}
