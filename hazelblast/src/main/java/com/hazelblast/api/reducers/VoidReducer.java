package com.hazelblast.api.reducers;

import java.util.Collection;

/**
 * A {@link Reducer} that doesn't do any reducing. Useful for ForkJoin methods that return void.
 *
 * @author Peter Veentjer.
 */
public class VoidReducer implements Reducer<Void> {

    public Void reduce(Collection<Void> results) {
        return null;
    }
}
