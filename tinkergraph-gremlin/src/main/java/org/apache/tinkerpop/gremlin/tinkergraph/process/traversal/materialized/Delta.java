/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import java.util.function.Function;

public class Delta<T> {

    public enum Change {
        ADD, DEL;

        public Change invert() {
            return this == ADD ? DEL : ADD;
        }
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

    public Delta<T> invert() {
        return new Delta<>(this.c.invert(), this.obj);
    }

    public boolean isAddition() {
        return this.c == Change.ADD;
    }

    public boolean isDeletion() {
        return this.c == Change.DEL;
    }

    public Change getChange() {
        return c;
    }

    public T getObj() {
        return obj;
    }
}

