package com.hazelblast.server.pojoslice;

import com.hazelblast.server.SliceConfig;
import com.hazelblast.server.SliceFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * A {@link com.hazelblast.server.SliceFactory} that where an ordinary Pojo can be used.
 * <p/>
 * This factory expects a pojoPu System property. This value should point to a class with a no arg constructor.
 * <p/>
 * All public fields of this pojo are services to be exposed to the outside world.
 *
 * @author Peter Veentjer.
 */
public final class PojoSliceFactory implements SliceFactory {

    public static final String POJO_SLICE_CLASS = "pojoSlice.class";

    private final static ILogger logger = Logger.getLogger(PojoSliceFactory.class.getName());
    private final Constructor pojoConstructor;

    /**
     * Creates a new PojoSliceFactory with the given pojoClazz. A no arg constructor will be used from this pojoClazz
     * to create instances with.
     *
     * @param pojoClazz the class to use to create Pojo's.
     * @throws NullPointerException if pojoClazz is null.
     */
    public PojoSliceFactory(Class pojoClazz) {
        notNull("pojoClazz", pojoClazz);

        try {
            pojoConstructor = pojoClazz.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(format("Can't find no-argument constructor on class '%s'", pojoClazz.getName()));
        }
    }

    /**
     * Creates a new PojoSliceFactory that uses the system property with key 'pojoSlice.class'  to determine which class
     * is being used to create the  {@link com.hazelblast.server.Slice}.
     */
    public PojoSliceFactory() {
        String pojoClassName = System.getProperty(POJO_SLICE_CLASS);
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("Using pojoSlice.class = '%s' from System.getProperty(%s)", pojoClassName, POJO_SLICE_CLASS));
        }

        if (pojoClassName == null) {
            throw new IllegalStateException(format("Failed to find property '%s' in System.properties", POJO_SLICE_CLASS));
        }

        try {
            Class pojoClazz = PojoSliceFactory.class.getClassLoader().loadClass(pojoClassName);
            pojoConstructor = pojoClazz.getConstructor();
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("pojoClass " + pojoClassName + " is not found", e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can't find no argument constructor on class " + pojoClassName);
        }
    }

    public PojoSlice create(SliceConfig sliceConfig) {
        try {
            Object pojo = pojoConstructor.newInstance();
            return new PojoSlice(pojo, sliceConfig);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to create a PojoSlice because the Pojo constructor " + pojoConstructor + " failed", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create a PojoSlice because the Pojo constructor " + pojoConstructor + " failed", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to create a PojoSlice because the Pojo constructor " + pojoConstructor + " failed", e);
        }
    }
}
