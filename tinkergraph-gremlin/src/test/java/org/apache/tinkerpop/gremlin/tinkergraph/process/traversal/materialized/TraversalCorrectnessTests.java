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

import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class TraversalCorrectnessTests {
    private Graph graph;

    @Before
    public void openGraph() {
        graph = TinkerGraph.open();
    }

    @After
    public void closeGraph() throws Exception {
        graph.close();
    }

    @Test
    public void VertexCount() {
        testTraversal(g -> g.V().count());
    }

    @Test
    public void EdgeCount() {
        testTraversal(g -> g.E().count());
    }

    @Test
    public void VertexSquaredCount() {
        testTraversal(g -> g.V().V().count());
    }

    @Test
    public void V() {
        testTraversal(g -> g.E());
    }

    @Test
    public void E() {
        testTraversal(g -> g.V());
    }

    @Test
    public void Constant() {
        testTraversal(g -> g.V().constant(1234));
    }

    @Test
    public void label() {
        testTraversal(g -> g.V().label());
    }

    @Test
    public void is1() {
        testTraversal(g -> g.V().constant(1).is(1));
    }

    @Test
    public void isNull() {
        testTraversal(g -> g.V().is(P.eq(null)));
    }

    @Test
    public void simplePath() {
        testTraversal(g -> g.V().V().simplePath());
    }

    @Test
    public void cyclicPath() {
        testTraversal(g -> g.V().V().cyclicPath());
    }

    @Test
    public void hasLabel() {
        testTraversal(g -> g.V().hasLabel("person"));
    }

    @Test
    public void hasKeyValue() {
        testTraversal(g -> g.V().has("name", "josh"));
    }

    @Test
    public void edgeHasCondition() {
        testTraversal(g -> g.E().has("weight", P.lt(0.5)));
    }

    @Test
    public void outE() {
        testTraversal(g -> g.V().outE());
    }

    @Test
    public void out() {
        testTraversal(g -> g.V().out());
    }

    @Test
    public void inE() {
        testTraversal(g -> g.V().inE());
    }

    @Test
    public void in() {
        testTraversal(g -> g.V().in());
    }

    @Test
    public void bothE() {
        testTraversal(g -> g.V().bothE());
    }

    @Test
    public void both() {
        testTraversal(g -> g.V().both());
    }

    @Test
    public void inV() {
        testTraversal(g -> g.E().inV());
    }

    @Test
    public void outV() {
        testTraversal(g -> g.E().outV());
    }

    @Test
    public void bothV() {
        testTraversal(g -> g.E().bothV());
    }

    @Test
    public void outE_otherV() {
        testTraversal(g -> g.V().outE().otherV());
    }

    @Test
    public void inE_otherV() {
        testTraversal(g -> g.V().inE().otherV());
    }

    @Test
    public void bothE_otherV() {
        testTraversal(g -> g.V().bothE().otherV());
    }

    @Test
    public void V_Properties() {
        testTraversal(g -> g.V().properties());
    }

    @Test
    public void V_PropertiesWithKey() {
        testTraversal(g -> g.V().properties("name"));
    }

    @Test
    public void E_Properties() {
        testTraversal(g -> g.E().properties());
    }

    @Test
    public void V_Properties_Key() {
        testTraversal(g -> g.V().properties().key());
    }

    @Test
    public void V_PropertiesWithKey_Key() {
        testTraversal(g -> g.V().properties("name").key());
    }

    @Test
    public void V_Properties_Value() {
        testTraversal(g -> g.V().properties().value());
    }

    @Test
    public void V_PropertiesWithKey_Value() {
        testTraversal(g -> g.V().properties("name").value());
    }

    @Test
    public void V_Age_Value_Sum() {
        testTraversal(g -> g.V().properties("age").value().sum());
    }

    @Test
    public void V_Age_Value_Count() {
        testTraversal(g -> g.V().properties("age").value().count());
    }

    @Test
    public void V_Age_Value_Min() {
        testTraversal(g -> g.V().properties("age").value().min());
    }

    @Test
    public void V_Age_Value_Max() {
        testTraversal(g -> g.V().properties("age").value().max());
    }

    @Test
    public void V_Age_Value_Mean() {
        testTraversal(g -> g.V().properties("age").value().mean());
    }

    @Test
    public void V_As_Select() {
        testTraversal(g -> g.V().as("a").select("a"));
    }

    @Test
    public void V_As_Out_Select() {
        testTraversal(g -> g.V().as("a").out().select("a"));
    }

    @Test
    public void V_WhereIdentity() {
        testTraversal(g -> g.V().where(__.identity()));
    }

    @Test
    public void V_WhereNone() {
        testTraversal(g -> g.V().where(__.identity().none()));
    }

    @Test
    public void V_WhereOutE() {
        testTraversal(g -> g.V().where(__.identity().outE()));
    }

    @Test
    public void V_NotIdentity() {
        testTraversal(g -> g.V().not(__.identity()));
    }

    @Test
    public void V_NotNone() {
        testTraversal(g -> g.V().not(__.identity().none()));
    }

    @Test
    public void V_WhereWhereIdentity() {
        testTraversal(g -> g.V().where(__.identity().where(__.identity())));
    }

    @Test
    public void V_WhereWhereNone() {
        testTraversal(g -> g.V().where(__.identity().where(__.identity().none())));
    }

    @Test
    public void V_NotNotIdentity() {
        testTraversal(g -> g.V().not(__.identity().not(__.identity())));
    }

    @Test
    public void V_NotNotNone() {
        testTraversal(g -> g.V().not(__.identity().not(__.identity().none())));
    }

    @Test
    public void V_WhereNotIdentity() {
        testTraversal(g -> g.V().where(__.identity().not(__.identity())));
    }

    @Test
    public void V_WhereNotNone() {
        testTraversal(g -> g.V().where(__.identity().not(__.identity().none())));
    }

    @Test
    public void V_NotWhereIdentity() {
        testTraversal(g -> g.V().not(__.identity().where(__.identity())));
    }

    @Test
    public void V_NotWhereNone() {
        testTraversal(g -> g.V().not(__.identity().where(__.identity().none())));
    }

    @Test
    public void V_NotOutE() {
        testTraversal(g -> g.V().not(__.identity().outE()));
    }

    @Test
    public void V_as_out_in_WhereEquals() {
        testTraversal(g -> g.V().as("first").out().in().as("second").where("first", P.eq("second")));
    }

    @Test
    public void V_WhereHasKeyValue() {
        testTraversal(g -> g.V().where(__.identity().has("name", "josh")));
    }

    @Test
    public void V_NotHasKeyValue() {
        testTraversal(g -> g.V().not(__.identity().has("name", "josh")));
    }

    @Test
    public void V_WhereNotHasKeyValue() {
        testTraversal(g -> g.V().where(__.identity().not(__.identity().has("name", "josh"))));
    }

    @Test
    public void V_NotWhereHasKeyValue() {
        testTraversal(g -> g.V().not(__.identity().where(__.identity().has("name", "josh"))));
    }

    private void testTraversal(Function<GraphTraversalSource, GraphTraversal<?, ?>> ts) {
        GraphTraversal<?, ?> t = ts.apply(graph.traversal());
        AbstractMaterializedView<?, ?> mView = new MaterializedView<>("myView", t.asAdmin().clone());
        MaterializedTraversalStore.getInstance().registerView(mView);

        GraphTraversal<?, ?> tp = ts.apply(graph.traversal()).path();
        AbstractMaterializedView<?, ?> mViewPath = new MaterializedView<>("myViewPath", tp.asAdmin().clone());
        MaterializedTraversalStore.getInstance().registerView(mViewPath);

        MaterializedTraversalSource g = AnonymousTraversalSource.traversal(MaterializedTraversalSource.class).withEmbedded(graph);

        buildModernTestGraph(g, () -> {
            assertEquals(ts.apply(graph.traversal()).toBulkSet(), g.view("myView").toBulkSet());
            assertEquals(ts.apply(graph.traversal()).path().toBulkSet(), g.view("myViewPath").toBulkSet());
        });
    }

    private void buildModernTestGraph(GraphTraversalSource g, Runnable tests) {
        Vertex marko = g.addV("person").property("name", "marko").property("age", 29).next(); tests.run();
        g.V(marko).drop().iterate(); tests.run();
        marko = g.addV("person").property("name", "marko").property("age", 29).next(); tests.run();
        Vertex vadas = g.addV("person").property("name", "vadas").property("age", 27).next(); tests.run();
        Vertex josh = g.addV("person").next(); tests.run();
        Vertex lop = g.addV("software").property("name", "lop").property("lang", "java").next(); tests.run();
        g.V(josh).property("name", "josh").property("age", 32).iterate(); tests.run();
        g.V(josh).property("name", "josh_temp").iterate(); tests.run();
        g.V(josh).property("name", "josh").iterate(); tests.run();
        Vertex ripple = g.addV("software").property("name", "ripple").property("lang", "java").next(); tests.run();
        Vertex peter = g.addV("person").property("name", "peter").property("age", 35).next(); tests.run();
        g.V(peter).drop().iterate(); tests.run();
        peter = g.addV("person").property("name", "peter").property("age", 35).next(); tests.run();

        Edge joshKnowsJosh = g.addE("knows").from(josh).to(josh).property("weight", 1.0).next(); tests.run();
        Edge markoKnowsVadas = g.addE("knows").from(marko).to(vadas).property("weight", 0.5).next(); tests.run();
        Edge markoKnowsJosh = g.addE("knows").from(marko).to(josh).property("weight", 1.0).next(); tests.run();
        Edge markoCreatedLop = g.addE("created").from(marko).to(lop).property("weight", 0.4).next(); tests.run();
        Edge peterCreatedLop = g.addE("created").from(peter).to(lop).property("weight", 0.2).next(); tests.run();
        Edge joshCreatedLop = g.addE("created").from(josh).to(lop).property("weight", 0.4).next(); tests.run();
        Edge joshCreatedRipple = g.addE("created").from(josh).to(ripple).property("weight", 1.0).next(); tests.run();
        g.E(joshCreatedRipple).drop().iterate(); tests.run();
        g.E(joshKnowsJosh).drop().iterate(); tests.run();
        joshCreatedRipple = g.addE("created").from(josh).to(ripple).property("weight", 1.0).next(); tests.run();

        g.V(vadas).drop().iterate(); tests.run();
        vadas = g.addV("person").property("name", "vadas").property("age", 27).next(); tests.run();
        markoKnowsVadas = g.addE("knows").from(marko).to(vadas).property("weight", 0.5).next(); tests.run();
    }
}
