package com.hazelblast.utils;

import static java.lang.String.format;

/**
 * A utility class to check arguments. There are a million libraries out there that to similar things, but we
 * want to keep the number of dependencies as low as possible.
 *
 * @author Peter Veentjer.
 */
public final class Arguments {


    public static <T> T notNull(String name, T object) {
        if (object == null) {
            throw new NullPointerException(format("'%s' can't be null", name));
        }
        return object;
    }
}
