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
package org.apache.tinkerpop.gremlin.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class MaterializedView<S> implements MutationListener {
    private final String name;
    protected final GraphTraversal<S, S> baseTraversal;
    private final List<Traverser.Admin<S>> results;

    public MaterializedView(String name, GraphTraversal<S, S> traversal) {
        this.name = name;
        this.baseTraversal = traversal;
        this.results = new ArrayList<>();
        initialize();
    }

    protected void recompute() {
        results.clear();
        initialize();
    }

    protected void addResult(Traverser.Admin<S> t) {
        results.add(t);
    }

    protected void removeResult(Traverser.Admin<S> t) {
        results.remove(t);
    }

    protected abstract void initialize();

    public Iterator<Traverser.Admin<S>> iterator() {
        return results.iterator();
    }

    public String getName() {
        return name;
    }
}
