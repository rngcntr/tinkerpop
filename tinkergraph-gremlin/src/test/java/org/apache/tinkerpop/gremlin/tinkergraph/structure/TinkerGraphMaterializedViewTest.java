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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.materialized.MaterializedView;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.TinkerFakeMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.TinkerMaterializedView;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TinkerGraphMaterializedViewTest {

    @Test
    public void EmptyGraphShouldHaveNoMaterializedViews() {
        final TinkerGraph graph = TinkerGraph.open();
        Set<String> mViewKeys = graph.getMaterializedViewKeys();
        assertTrue(mViewKeys.isEmpty());
    }

    @Test
    public void RegisteredMaterializedViewIsSavedWithName() {
        final TinkerGraph graph = TinkerGraph.open();
        MaterializedView<Vertex,Vertex> mView = new TinkerFakeMaterializedView<>("myView", graph.traversal().V());
        graph.registerMaterializedView(mView);
        assertEquals(mView, graph.materializedView("myView"));
    }

    @Test
    public void FakeMaterializedViewReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        MaterializedView<Vertex,Vertex> mView = new TinkerFakeMaterializedView<>("myView", graph.traversal().V());
        graph.registerMaterializedView(mView);
        populateExampleGraph(graph);
        long expectedCount = graph.traversal().V().count().next();
        long actualCount = graph.traversal().mView("myView").count().next();
        Assert.assertEquals(expectedCount, actualCount);
    }

    @Test
    public void SimpleMaterializedViewReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        MaterializedView<Vertex,Vertex> mView = new TinkerMaterializedView<>("myView", graph.traversal().V());
        graph.registerMaterializedView(mView);
        populateExampleGraph(graph);

        long expectedCount = graph.traversal().V().count().next();
        long actualCount = graph.traversal().mView("myView").count().next();

        Assert.assertEquals(expectedCount, actualCount);
    }

    @Test
    public void CountMaterializedViewReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        MaterializedView<Vertex,Long> mView = new TinkerMaterializedView<>("myView", graph.traversal().V().count());
        graph.registerMaterializedView(mView);
        populateExampleGraph(graph);

        long expectedCount = graph.traversal().V().count().next();
        long actualCount = (long) graph.traversal().mView("myView").next();

        Assert.assertEquals(expectedCount, actualCount);
    }

    public void populateExampleGraph(Graph graph) {
        graph.traversal().addV("person").property("name", "Florian H.").iterate();
        graph.traversal().addV("person").property("name", "Kadir B.").iterate();
        graph.traversal().addV("person").property("name", "Phillip K.").iterate();
        graph.traversal().addV("person").property("name", "Florian G.").iterate();
    }
}
