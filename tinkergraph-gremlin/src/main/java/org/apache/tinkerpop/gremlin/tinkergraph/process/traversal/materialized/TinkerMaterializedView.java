package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.MaterializedView;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TinkerMaterializedView<S,E> extends MaterializedView<S,E> {

    private TinkerMaterializedSubStep<S,?> materializedStartStep;
    private List<TinkerMaterializedSubStep<?,?>> materializedSteps;
    private TinkerMaterializedSubStep<?,E> materializedEndStep;

    public TinkerMaterializedView(String name, GraphTraversal<S, E> traversal) {
        super(name, traversal);
        materializedSteps = new ArrayList<>();
        Step s = traversal.asAdmin().getStartStep();
        TinkerMaterializedSubStep ms = TinkerMaterializedSubStep.of(s);
        materializedSteps.add(ms);
        materializedStartStep = ms;
        while (s.getNextStep() != null && !(s.getNextStep() instanceof EmptyStep)) {
            s = s.getNextStep();
            TinkerMaterializedSubStep newMs = TinkerMaterializedSubStep.of(s);
            newMs.setPreviousStep(ms);
            ms.setNextStep(newMs);
            materializedSteps.add(newMs);
            ms = newMs;
        }
        materializedEndStep = ms;
    }

    public Iterator<Traverser.Admin<E>> iterator() {
        return materializedEndStep.outputs();
    }

    @Override
    protected void initialize() {
        materializedStartStep.registerInputDelta(new Delta<>(Delta.Change.ADD, null));
    }

    @Override
    public void vertexAdded(Vertex vertex) {
        materializedSteps.forEach(s -> s.vertexAdded(vertex));
    }

    @Override
    public void vertexRemoved(Vertex vertex) {
        materializedSteps.forEach(s -> s.vertexRemoved(vertex));
    }

    @Override
    public void vertexPropertyChanged(Vertex element, VertexProperty oldValue, Object setValue, Object... vertexPropertyKeyValues) {
        materializedSteps.forEach(s -> s.vertexPropertyChanged(element, oldValue, setValue, vertexPropertyKeyValues));
    }

    @Override
    public void vertexPropertyRemoved(VertexProperty vertexProperty) {
        materializedSteps.forEach(s -> s.vertexPropertyRemoved(vertexProperty));
    }

    @Override
    public void edgeAdded(Edge edge) {
        materializedSteps.forEach(s -> s.edgeAdded(edge));
    }

    @Override
    public void edgeRemoved(Edge edge) {
        materializedSteps.forEach(s -> s.edgeRemoved(edge));
    }

    @Override
    public void edgePropertyChanged(Edge element, Property oldValue, Object setValue) {
        materializedSteps.forEach(s -> s.edgePropertyChanged(element, oldValue, setValue));
    }

    @Override
    public void edgePropertyRemoved(Edge element, Property property) {
        materializedSteps.forEach(s -> s.edgePropertyRemoved(element, property));
    }

    @Override
    public void vertexPropertyPropertyChanged(VertexProperty element, Property oldValue, Object setValue) {
        materializedSteps.forEach(s -> s.vertexPropertyPropertyChanged(element, oldValue, setValue));
    }

    @Override
    public void vertexPropertyPropertyRemoved(VertexProperty element, Property property) {
        materializedSteps.forEach(s -> s.vertexPropertyPropertyRemoved(element, property));
    }
}
