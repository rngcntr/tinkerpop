package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TraversalPerformanceTests {

    @Test
    public void CreateTinkerGraphTest() {
        final TinkerGraph graph = TinkerGraph.open();
        assertNotNull(graph);
        graph.close();
    }

    @Test
    public void RegisteredMaterializedViewIsSavedWithName() {
        final TinkerGraph graph = TinkerGraph.open();
        AbstractMaterializedView<Vertex,Vertex> mView = new MaterializedView<>("myView", graph.traversal().V().asAdmin());
        MaterializedTraversalStore.getInstance().registerView(mView);
        assertEquals(mView, MaterializedTraversalStore.getInstance().getView("myView"));
        graph.close();
    }

    @Test
    public void CountMaterializedViewReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        AbstractMaterializedView<?,?> mView = new MaterializedView<>("myView", graph.traversal().V().identity().identity().identity().identity().identity().identity().count().asAdmin());
        MaterializedTraversalStore.getInstance().registerView(mView);
        MaterializedTraversalSource g = AnonymousTraversalSource.traversal(MaterializedTraversalSource.class).withEmbedded(graph);

        int numVertices = 2_000;

        long insert = 0, normal = 0, view = 0;
        for (int i = 0; i < numVertices; ++i) {
            insert -= System.nanoTime();
            g.addV("item").property("id", i).iterate();
            insert += System.nanoTime();

            normal -= System.nanoTime();
            long actualCount = g.V().identity().identity().identity().identity().identity().identity().count().next();
            normal += System.nanoTime();

            view -= System.nanoTime();
            long viewCount = (Long) g.view("myView").next();
            view += System.nanoTime();

            assertEquals(actualCount, viewCount);
        }

        System.out.printf("Insert: %5.3fs\n", (double) insert / 1_000_000_000);
        System.out.printf("Normal: %5.3fs\n", (double) normal / 1_000_000_000);
        System.out.printf("  View: %5.3fs\n", (double) view / 1_000_000_000);
        graph.close();
    }

    @Test
    public void CountNormalTraversalReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();

        int numVertices = 2_000;

        long insert = 0, normal = 0;
        for (int i = 0; i < numVertices; ++i) {
            insert -= System.nanoTime();
            g.addV("item").property("id", i).iterate();
            insert += System.nanoTime();

            normal -= System.nanoTime();
            long actualCount = g.V().identity().identity().identity().identity().identity().identity().count().next();
            normal += System.nanoTime();

            assertEquals(i + 1, actualCount);
        }

        System.out.printf("Insert: %5.3fs\n", (double) insert / 1_000_000_000);
        System.out.printf("Normal: %5.3fs\n", (double) normal / 1_000_000_000);
        graph.close();
    }

    @Test
    public void SquaredCountMaterializedViewReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        AbstractMaterializedView<?,?> mView = new MaterializedView<>("myView", graph.traversal().V().V().count().asAdmin());
        MaterializedTraversalStore.getInstance().registerView(mView);
        MaterializedTraversalSource g = AnonymousTraversalSource.traversal(MaterializedTraversalSource.class).withEmbedded(graph);

        int numVertices = 100;

        long insert = 0, view = 0;
        for (int i = 0; i < numVertices; ++i) {
            insert -= System.nanoTime();
            g.addV("item").property("id", i).iterate();
            insert += System.nanoTime();

            view -= System.nanoTime();
            long viewCount = (Long) g.view("myView").next();
            view += System.nanoTime();

            assertEquals((i + 1) * (i + 1), viewCount);
        }

        System.out.printf("Insert: %5.3fs\n", (double) insert / 1_000_000_000);
        System.out.printf("  View: %5.3fs\n", (double) view / 1_000_000_000);
        graph.close();
    }

    @Test
    public void SquaredCountNormalTraversalReturnsCorrectResult() {
        final TinkerGraph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();

        int numVertices = 100;

        long insert = 0, normal = 0;
        for (int i = 0; i < numVertices; ++i) {
            insert -= System.nanoTime();
            g.addV("item").property("id", i).iterate();
            insert += System.nanoTime();

            normal -= System.nanoTime();
            long actualCount = g.V().V().count().next();
            normal += System.nanoTime();

            assertEquals((i + 1) * (i + 1), actualCount);
        }

        System.out.printf("Insert: %5.3fs\n", (double) insert / 1_000_000_000);
        System.out.printf("Normal: %5.3fs\n", (double) normal / 1_000_000_000);
        graph.close();
    }
}
