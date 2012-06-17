package com.hazelblast.api;

import com.hazelblast.loadbalancers.RoundRobinLoadBalancer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that can be placed on a method to indicate that a call is allowed to execute on any of the
 * nodes. So it doesn't matter which node is going to process that call.
 *
 * @author Peter Veentjer.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LoadBalanced{

    long timeoutMs() default 60 * 1000;

    Class<? extends LoadBalancer> loadBalancer() default RoundRobinLoadBalancer.class;
}
