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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractMaterializedView<S,E> implements MutationListener {
    private final String name;
    protected final Traversal.Admin<S,E> baseTraversal;
    protected final List<Traverser.Admin<E>> results;

    public AbstractMaterializedView(String name, Traversal.Admin<S,E> baseTraversal) {
        this.name = name;
        this.baseTraversal = baseTraversal;
        this.results = new ArrayList<>();
    }

    public void registerOutputDelta(Delta<Traverser.Admin<E>> inputChange) {
        if (inputChange.isAddition()) {
            results.add(inputChange.getObj());
        } else {
            Util.removeFirst(results, t -> t.equals(inputChange.getObj()));
        }
    }

    protected abstract void initialize();

    public String getName() {
        return name;
    }

    public Iterator<Traverser.Admin<E>> iterator() {
        List<Traverser.Admin<E>> clones = new ArrayList<>(results.size());
        for (Traverser.Admin<E> t : results) {
            clones.add(t.clone().asAdmin());
        }
        return clones.iterator();
    }
}
