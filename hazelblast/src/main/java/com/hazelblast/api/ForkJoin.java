package com.hazelblast.api;


import com.hazelblast.api.reducers.Reducer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that can be placed on a method to indicate that the method is going to be executed o al
 * nodes and the results are aggregated.
 *
 * @author Peter Veentjer.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ForkJoin {

    Class<? extends Reducer> reducer();
}
