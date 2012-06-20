package com.hazelblast.client.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that needs to be placed on an interface to indicate that this service will be a distributed service.
 * All methods of a distributed service will either be loadbalanced, or partitioned.
 * <p/>
 * Annotations do not inherit, so if you are extending an interface that has this annotation, the extended interface
 * also needs to have this {@link DistributedService} annotation.
 *
 * @author Peter Veentjer.
 * @see LoadBalanced
 * @see Partitioned
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DistributedService {
}
