package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.MaterializedView;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class TinkerMaterializedCountStep<T> extends TinkerMaterializedSubStep<T,T> {
    private long count;
    private Traverser.Admin lastOutput;
    final TraverserGenerator generator;

    protected <E, S> TinkerMaterializedCountStep(MaterializedView mv, Step<T,T> originalStep) {
        super(mv, originalStep);
        count = 0L;
        generator = originalStep.getTraversal().getTraverserGenerator();
        lastOutput = generator.generate(count, null, 1l);
        deltaOutput(new Delta<>(Delta.Change.ADD, lastOutput));
    }

    @Override
    public void registerInputDelta(Delta inputChange) {
        count += inputChange.getChange() == Delta.Change.ADD ? 1 : -1;
        deltaOutput(new Delta<>(Delta.Change.DEL, lastOutput));
        lastOutput = generator.generate(count, null, 1L);
        deltaOutput(new Delta<>(Delta.Change.ADD, lastOutput));
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
