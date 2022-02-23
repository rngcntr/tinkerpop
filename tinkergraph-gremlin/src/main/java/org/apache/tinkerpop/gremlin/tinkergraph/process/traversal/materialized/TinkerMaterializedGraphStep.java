package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.MaterializedView;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.*;

public class TinkerMaterializedGraphStep<S, E extends Element> extends TinkerMaterializedSubStep<S,E> {
    Class<E> outputType;
    GraphStep<S,E> originalStep;

    protected TinkerMaterializedGraphStep(MaterializedView mv, GraphStep<S,E> originalStep) {
        super(mv, originalStep);
        this.outputType = originalStep.getReturnClass();
        this.originalStep = originalStep;
    }

    @Override
    public void registerInputDelta(Delta inputChange) {
        originalStep.getTraversal().getGraph().get().vertices().forEachRemaining(vertex ->
            deltaOutput(new Delta(Delta.Change.ADD, originalStep.getTraversal().getTraverserGenerator().generate(vertex, (Step) originalStep, 1l)))
        );
    }

    @Override
    public void vertexAdded(Vertex vertex) {
        deltaOutput(new Delta(Delta.Change.ADD, originalStep.getTraversal().getTraverserGenerator().generate(vertex, (Step) originalStep, 1l)));
    }

    @Override
    public void vertexRemoved(Vertex vertex) {
        deltaOutput(new Delta(Delta.Change.DEL, originalStep.getTraversal().getTraverserGenerator().generate(vertex, (Step) originalStep, 1l)));
    }

    @Override
    public void vertexPropertyChanged(Vertex element, VertexProperty oldValue, Object setValue, Object... vertexPropertyKeyValues) {

    }

    @Override
    public void vertexPropertyRemoved(VertexProperty vertexProperty) {

    }

    @Override
    public void edgeAdded(Edge edge) {

    }

    @Override
    public void edgeRemoved(Edge edge) {

    }

    @Override
    public void edgePropertyChanged(Edge element, Property oldValue, Object setValue) {

    }

    @Override
    public void edgePropertyRemoved(Edge element, Property property) {

    }

    @Override
    public void vertexPropertyPropertyChanged(VertexProperty element, Property oldValue, Object setValue) {

    }

    @Override
    public void vertexPropertyPropertyRemoved(VertexProperty element, Property property) {

    }
}
