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

import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.MaterializedViewStep;

public class MaterializedTraversalSource extends GraphTraversalSource {
    public MaterializedTraversalSource(Graph graph, TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
        addListeners();
    }

    public MaterializedTraversalSource(Graph graph) {
        super(graph);
        addListeners();
    }

    public MaterializedTraversalSource(RemoteConnection connection) {
        super(connection);
        addListeners();
    }

    private void addListeners() {
        final EventStrategy.Builder strategyBuilder = EventStrategy.build();
        for (AbstractMaterializedView mv : MaterializedTraversalStore.getInstance().getViews()) {
            strategyBuilder.addListener(mv);
        }
        strategies.addStrategies(strategyBuilder.create());
    }

    public <S> GraphTraversal<S,S> view(String name) {
        final GraphTraversalSource clone = this.clone();
        final AbstractMaterializedView<?,?> mView = MaterializedTraversalStore.getInstance().getView(name);
        clone.getBytecode().addStep(MaterializedViewStep.SYMBOL, mView.getName());
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new MaterializedViewStep(traversal, mView));
    }
}
