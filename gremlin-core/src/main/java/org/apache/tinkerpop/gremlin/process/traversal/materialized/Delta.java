package org.apache.tinkerpop.gremlin.process.traversal.materialized;

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

    public Change getChange() {
        return c;
    }

    public T getObj() {
        return obj;
    }
}
