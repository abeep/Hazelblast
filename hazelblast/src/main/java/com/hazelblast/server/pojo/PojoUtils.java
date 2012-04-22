package com.hazelblast.server.pojo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains some utility functions for using Pojo's.
 *
 * @author Peter Veentjer
 */
public class PojoUtils {

    public static Map<String, Field> getPublicFields(Class targetClazz) {
        Map<String, Field> fields = new HashMap<String, Field>();
        getPublicFields(targetClazz,fields);
        return fields;
    }

    private static void getPublicFields(Class targetClass, Map<String, Field> fields) {
        for (Field field : targetClass.getFields()) {
            if (!fields.containsKey(field.getName())) {
                if (Modifier.isPublic(field.getModifiers())) {
                    fields.put(field.getName(), field);
                }
            }
        }

        Class superClass = targetClass.getSuperclass();
        if(superClass!=null){
            getPublicFields(superClass,fields);
        }
    }

    /**
     * Looks up a public void method with the given signature. It will look up the class hierarchy
     * to find it.
     *
     * @param targetClazz the Class that contains the method.
     * @param methodName  the name of the method
     * @param argTypes    the types of the arguments
     * @return the found method, or null if nothing is found.
     */
    public static Method getPublicVoidMethod(Class targetClazz, String methodName, Class... argTypes) {
        Method method;
        try {
            method = targetClazz.getMethod(methodName, argTypes);
        } catch (NoSuchMethodException e) {
            Class superClass = targetClazz.getSuperclass();
            if (superClass == null) {
                return null;
            } else {
                return getPublicVoidMethod(superClass, methodName, argTypes);
            }
        }

        if (!method.getReturnType().equals(Void.TYPE)) {
            throw new IllegalArgumentException("Method '" + method + "' does not return void");
        }

        if (!Modifier.isPublic(method.getModifiers())) {
            throw new IllegalArgumentException("Method '" + method + "' is not public");
        }

        if (Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException("Method '" + method + "' is not an instance method, but static");
        }

        return method;
    }
}
