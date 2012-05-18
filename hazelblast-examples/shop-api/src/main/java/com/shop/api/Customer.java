package com.shop.api;

import java.io.Serializable;

import static com.hazelblast.utils.Arguments.notNull;

public final class Customer implements Serializable {
    private final String id;
    private boolean fired = false;
    private String name;

    public Customer(String id) {
        this.id = notNull("id", id);
    }

    public String getId() {
        return id;
    }

    public boolean isFired() {
        return fired;
    }

    public void setFired(boolean fired) {
        this.fired = fired;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id='" + id + '\'' +
                ", fired=" + fired +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Customer customer = (Customer) o;

        return customer.id.equals(id);
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
