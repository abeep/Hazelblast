package com.hazelblast.server.pojo;

import com.hazelblast.server.ServiceContext;
import com.hazelblast.server.ServiceContextFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Level;

import static com.hazelblast.utils.Arguments.notNull;
import static java.lang.String.format;

/**
 * A {@link com.hazelblast.server.ServiceContextFactory} that where an ordinary Pojo can be used.
 * <p/>
 * This factory expects a pojoPu System property. This value should point to a class with a no arg constructor.
 * <p/>
 * All public fields of this pojo are services to be exposed to the outside world.
 *
 * @author Peter Veentjer.
 */
public final class PojoServiceContextFactory implements ServiceContextFactory {

    public static final String POJO_SERVICE_CONTEXT_CLASS = "pojoServiceContext.class";

    private final static ILogger logger = Logger.getLogger(PojoServiceContextFactory.class.getName());
    private final Constructor constructor;

    /**
     * Creates a new PojoServiceContextFactory with the given pojoClazz. A no arg constructor will be used from this pojoClazz
     * to create instances with.
     *
     * @param pojoClazz the class to use to create Pojo's.
     * @throws NullPointerException if pojoClazz is null.
     */
    public PojoServiceContextFactory(Class pojoClazz) {
        notNull("pojoClazz",pojoClazz);

        try {
            constructor = pojoClazz.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can't find no argument constructor on class " + pojoClazz);
        }
    }

    /**
     * Creates a new PojoServiceContextFactory that uses the system property with key 'pojoServiceContext.class'  to determine which class
     * is being used to create the  {@link ServiceContext}.
     */
    public PojoServiceContextFactory() {
        String pojoClassName = System.getProperty(POJO_SERVICE_CONTEXT_CLASS);
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, format("Using pojoClassName = '%s' from System.getProperty(%s)", pojoClassName, POJO_SERVICE_CONTEXT_CLASS));
        }

        if (pojoClassName == null) {
            throw new IllegalStateException(format("Failed to find property '%s' in System.properties", POJO_SERVICE_CONTEXT_CLASS));
        }

        try {
            Class pojoClazz = PojoServiceContextFactory.class.getClassLoader().loadClass(pojoClassName);
            constructor = pojoClazz.getConstructor();
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("pojoClass " + pojoClassName + " is not found", e);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Can't find no argument constructor on class " + pojoClassName);
        }
    }

    public ServiceContext create() {
        if(logger.isLoggable(Level.FINE)){
            logger.log(Level.FINE,"creating PojoServiceContext using Pojo-constructor "+constructor);
        }

        try {
            Object target = constructor.newInstance();
            return new PojoServiceContext(target);
        } catch (InstantiationException e) {
            throw new RuntimeException("Failed to create a PojoProcessingUnit because the Pojo constructor " + constructor + " failed", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create a PojoProcessingUnit because the Pojo constructor " + constructor + " failed", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Failed to create a PojoProcessingUnit because the Pojo constructor " + constructor + " failed", e);
        }
    }
}
