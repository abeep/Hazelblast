package com.hazelblast.api.reducers;

import java.util.Collection;

/**
 * Responsible for reducing the results to a single result when a ForkJoin method has been called.
 * @param <T>
 *
 * @author Peter Veentjer.
 */
public interface Reducer<T> {
    T reduce(Collection<T> results) ;
}
