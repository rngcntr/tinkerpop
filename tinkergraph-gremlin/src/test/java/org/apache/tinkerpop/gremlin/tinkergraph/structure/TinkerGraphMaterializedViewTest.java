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
        graph.registerMaterializedView(new TinkerFakeMaterializedView<>("myView", graph.traversal().V()));

        for (int i = 0; i < 10; ++i) {
            graph.traversal().addV("item").property("id", i).iterate();

            long expectedCount = graph.traversal().V().count().next();
            long actualCount = graph.traversal().mView("myView").count().next();

            Assert.assertEquals(expectedCount, actualCount);
        }
    }

    @Test
    public void SimpleMaterializedViewReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        graph.registerMaterializedView(new TinkerMaterializedView<>("myView", graph.traversal().V()));

        for (int i = 0; i < 10; ++i) {
            graph.traversal().addV("item").property("id", i).iterate();

            long expectedCount = graph.traversal().V().count().next();
            long actualCount = graph.traversal().mView("myView").count().next();

            Assert.assertEquals(expectedCount, actualCount);
        }
    }

    @Test
    public void CountMaterializedViewReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        graph.registerMaterializedView(new TinkerMaterializedView<>("myView", graph.traversal().V().count()));

        long insertTime = 0;
        long queryTime = 0;

        insertTime -= System.nanoTime();
        for (int i = 0; i < 1_000_000; ++i) {
            graph.traversal().addV("item").property("id", i).iterate();
        }
        insertTime += System.nanoTime();

        long sum = 0;
        queryTime -= System.nanoTime();
        for (int i = 0; i < 1_000_000; ++i) {
            sum += graph.traversal().V().count().next();
        }
        queryTime += System.nanoTime();
        System.out.printf("Insert Time: %.0f ms\n", 1.0 * insertTime / 1_000_000);
        System.out.printf(" Query Time: %.0f ms\n", 1.0 * queryTime / 1_000_000);
        Assert.assertEquals(sum, sum + 1 - 1);
    }

    @Test
    public void CountMaterializedViewReturnsCorrectResult2() {
        final TinkerGraph graph = TinkerGraph.open();
        graph.registerMaterializedView(new TinkerMaterializedView<>("myView", graph.traversal().V().count()));

        long insertTime = 0;
        long queryTime = 0;

        insertTime -= System.nanoTime();
        for (int i = 0; i < 1_000; ++i) {
            graph.traversal().addV("item").property("id", i).iterate();
        }
        insertTime += System.nanoTime();
        long sum = 0;
        queryTime -= System.nanoTime();
        for (int i = 0; i < 1_000; ++i) {
            sum += (long) graph.traversal().mView("myView").next();
        }
        queryTime += System.nanoTime();

        System.out.printf("Insert Time: %.0f ms\n", 1.0 * insertTime / 1_000_000);
        System.out.printf(" Query Time: %.0f ms\n", 1.0 * queryTime / 1_000_000);
        Assert.assertEquals(sum, sum + 1 - 1);
    }
}
