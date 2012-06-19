package com.hazelblast.utils;

import static java.lang.String.format;

/**
 * A utility class to check arguments. There are a million libraries out there that to similar things, but we
 * want to keep the number of dependencies as low as possible.
 *
 * @author Peter Veentjer.
 */
public final class Arguments {

    /**
     * Verifies that an object reference is not null.
     *
     * @param name the name of the object reference.
     * @param object  the object reference
     * @param <T> the type of the object reference
     * @return  the object reference (will never be null)
     * @throws NullPointerException if object is null.
     */
    public static <T> T notNull(String name, T object) {
        if (object == null) {
            throw new NullPointerException(format("'%s' can't be null", name));
        }
        return object;
    }
}
