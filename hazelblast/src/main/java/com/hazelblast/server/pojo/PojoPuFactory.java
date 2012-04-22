package com.hazelblast.server.pojo;

import com.hazelblast.api.ProcessingUnit;
import com.hazelblast.api.PuFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Level;

import static java.lang.String.format;

/**
 * A {@link PuFactory} that where an ordinary Pojo can be used.
 * <p/>
 * This factory expects a pojoPu System property. This value should point to a class with a no arg constructor.
 * <p/>
 * All public fields of this pojo are services to be exposed to the outside world.
 *
 * @author Peter Veentjer.
 */
public final class PojoPuFactory implements PuFactory {

    public static final String POJO_PU_CLASS = "pojoPu.class";

    private final static ILogger logger = Logger.getLogger(PojoPuFactory.class.getName());
    private final Constructor constructor;

    /**
     * Creates a new PojoPuFactory with the given pojoClazz. A no arg constructor will be looked up on this pojoClazz
     * to create instances with.
     *
     * @param pojoClazz the class to use to create Pojo's.
     * @throws NullPointerException if pojoClazz is null.
     */
    public PojoPuFactory(Class pojoClazz) {
        if (pojoClazz == null) {
            throw new NullPointerException("pojoClazz can't be null");
        }

        try {
            constructor = pojoClazz.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can't find no argument constructor on class " + pojoClazz);
        }
    }

    /**
     * Creates a new PojoPuFactory that uses the system property with key 'pojoPu.class'  to determine which class
     * is being used to create the processing unit.
     */
    public PojoPuFactory() {
        String pojoClassName = System.getProperty(POJO_PU_CLASS);
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("Using pojoClassName = '%s' from System.getProperty(%s)", pojoClassName, POJO_PU_CLASS));
        }

        if (pojoClassName == null) {
            throw new IllegalStateException(format("Failed to find property '%s' in System.properties", POJO_PU_CLASS));
        }

        try {
            Class pojoClazz = PojoPuFactory.class.getClassLoader().loadClass(pojoClassName);
            constructor = pojoClazz.getConstructor();
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("pojoClass " + pojoClassName + " is not found", e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can't find no argument constructor on class " + pojoClassName);
        }
    }

    public ProcessingUnit create() {
        if(logger.isLoggable(Level.FINE)){
            logger.log(Level.FINE,"creating PojoPu using Pojo-constructor "+constructor);
        }

        try {
            Object target = constructor.newInstance();
            return new PojoPu(target);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to create a PojoProcessingUnit because the Pojo constructor " + constructor + " failed", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create a PojoProcessingUnit because the Pojo constructor " + constructor + " failed", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to create a PojoProcessingUnit because the Pojo constructor " + constructor + " failed", e);
        }
    }
}
