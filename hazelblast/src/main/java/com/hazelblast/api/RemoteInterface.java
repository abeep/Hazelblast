package com.hazelblast.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that needs to be placed on an interface so that it will be exposed remotely.
 *
 * Annotations do not inherit, so if you are extending an interface that has this annotation, the extended interface
 * also needs to have this remoteInterface annotation.
 *
 * @author Peter Veentjer.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface RemoteInterface {
}
