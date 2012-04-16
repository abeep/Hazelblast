package com.hazelblast.api;


import com.hazelblast.api.reducers.Reducer;
import com.hazelblast.api.reducers.VoidReducer;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ForkJoin {

    Class<? extends Reducer> reducer();
}
