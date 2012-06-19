package com.hazelblast.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Annotation that can be placed on a method to indicate that the call should be send to a specific partition.
 *
 * @author Peter Veentjer.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Partitioned {
    long timeoutMs() default 60 * 1000;
    boolean interruptOnTimeout() default true;
}