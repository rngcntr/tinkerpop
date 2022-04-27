package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import java.util.function.Function;

public class Delta<T> {

    public enum Change {
        ADD, DEL
    }

    private final Change c;
    private final T obj;

    public Delta(Change c, T obj) {
        this.c = c;
        this.obj = obj;
    }

    public static <T> Delta<T> add(T obj) {
        return new Delta<>(Change.ADD, obj);
    }

    public static <T> Delta<T> del(T obj) {
        return new Delta<>(Change.DEL, obj);
    }

    public <R> Delta<R> map(Function<T,R> map) {
        return new Delta<>(this.c, map.apply(this.obj));
    }

    public Change getChange() {
        return c;
    }

    public T getObj() {
        return obj;
    }
}

