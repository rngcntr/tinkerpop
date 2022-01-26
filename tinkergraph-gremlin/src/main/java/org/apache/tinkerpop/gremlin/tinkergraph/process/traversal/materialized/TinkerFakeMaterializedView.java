package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.MaterializedView;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.List;

public class TinkerFakeMaterializedView<S,E> extends MaterializedView<S,E> {

    public TinkerFakeMaterializedView(String name, GraphTraversal<S, E> traversal) {
        super(name, traversal);
        initialize();
    }

    @Override
    protected void initialize() {
        baseTraversal.asAdmin().reset();
        while (baseTraversal.hasNext()) {
            addResult(baseTraversal.asAdmin().nextTraverser());
        }
    }

    private void recompute() {
        List<Traverser.Admin<E>> results = new ArrayList<>();
        iterator().forEachRemaining(results::add);
        results.iterator().forEachRemaining(this::removeResult);
        initialize();
    }

    @Override
    public void vertexAdded(Vertex vertex) {
        recompute();
    }

    @Override
    public void vertexRemoved(Vertex vertex) {
        recompute();
    }

    @Override
    public void vertexPropertyChanged(Vertex element, VertexProperty oldValue, Object setValue, Object... vertexPropertyKeyValues) {
        recompute();
    }

    @Override
    public void vertexPropertyRemoved(VertexProperty vertexProperty) {
        recompute();
    }

    @Override
    public void edgeAdded(Edge edge) {
        recompute();
    }

    @Override
    public void edgeRemoved(Edge edge) {
        recompute();
    }

    @Override
    public void edgePropertyChanged(Edge element, Property oldValue, Object setValue) {
        recompute();
    }

    @Override
    public void edgePropertyRemoved(Edge element, Property property) {
        recompute();
    }

    @Override
    public void vertexPropertyPropertyChanged(VertexProperty element, Property oldValue, Object setValue) {
        recompute();
    }

    @Override
    public void vertexPropertyPropertyRemoved(VertexProperty element, Property property) {
        recompute();
    }
}
