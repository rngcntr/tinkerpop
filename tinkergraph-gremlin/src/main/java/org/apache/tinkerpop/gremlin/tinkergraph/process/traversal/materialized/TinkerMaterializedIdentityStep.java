package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class TinkerMaterializedIdentityStep<T> extends TinkerMaterializedSubStep<T,T> {
    protected TinkerMaterializedIdentityStep() {
    }

    @Override
    public void registerInputDelta(Delta inputChange) {
        deltaOutput(inputChange);
    }

    @Override
    public void vertexAdded(Vertex vertex) { }

    @Override
    public void vertexRemoved(Vertex vertex) { }

    @Override
    public void vertexPropertyChanged(Vertex element, VertexProperty oldValue, Object setValue, Object... vertexPropertyKeyValues) { }

    @Override
    public void vertexPropertyRemoved(VertexProperty vertexProperty) { }

    @Override
    public void edgeAdded(Edge edge) { }

    @Override
    public void edgeRemoved(Edge edge) { }

    @Override
    public void edgePropertyChanged(Edge element, Property oldValue, Object setValue) { }

    @Override
    public void edgePropertyRemoved(Edge element, Property property) { }

    @Override
    public void vertexPropertyPropertyChanged(VertexProperty element, Property oldValue, Object setValue) { }

    @Override
    public void vertexPropertyPropertyRemoved(VertexProperty element, Property property) { }
}
