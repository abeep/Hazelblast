package com.hazelblast.server.springslice;

public class BeanInfo<T> {

    public final String name;
    public final T bean;

    public BeanInfo(String name, T bean) {
        this.name = name;
        this.bean = bean;
    }
}
