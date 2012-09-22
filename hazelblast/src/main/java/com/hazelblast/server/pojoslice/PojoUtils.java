package com.hazelblast.server.pojoslice;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static com.hazelblast.utils.Arguments.notNull;

/**
 * Contains some utility functions for using Pojo's.
 *
 * @author Peter Veentjer
 */
public final class PojoUtils {

    /**
     * Gets all public fields of an class, including those of the super classes.
     *
     * @param targetClazz the class to get the public fields for.
     * @return a Map containing all public fields of the targetClazz.
     */
    public static Map<String, Field> getServiceFields(Class targetClazz) {
        notNull("targetClass", targetClazz);

        Map<String, Field> fields = new HashMap<String, Field>();
        getServiceFields(targetClazz, fields);
        return fields;
    }

    private static void getServiceFields(Class targetClass, Map<String, Field> fields) {
        for (Field field : targetClass.getDeclaredFields()) {
            if (!fields.containsKey(field.getName())) {
                Exposed service = field.getAnnotation(Exposed.class);
                if (service != null) {
                    field.setAccessible(true);
                    fields.put(field.getName(), field);
                }
            }
        }

        Class superClass = targetClass.getSuperclass();
        if (superClass != null) {
            getServiceFields(superClass, fields);
        }
    }

    public static boolean matches(Method method, String methodName, String[] argTypes) {
        if (!method.getName().equals(methodName)) {
            return false;
        }

        Class[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != argTypes.length) {
            return false;
        }

        for (int argIndex = 0; argIndex < parameterTypes.length; argIndex++) {
            String argType = argTypes[argIndex];
            String paramType = parameterTypes[argIndex].getCanonicalName();
            if (!argType.equals(paramType)) {
                return false;
            }
        }

        return true;
    }
}
