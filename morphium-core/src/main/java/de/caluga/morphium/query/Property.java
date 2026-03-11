package de.caluga.morphium.query;

import java.io.Serializable;

@FunctionalInterface
public interface Property<T, R> extends Serializable {
    R apply(T t);
}

