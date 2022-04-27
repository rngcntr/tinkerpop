package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.commons.collections.iterators.ReverseListIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util.MaterializedSubStep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MaterializedView<S,E> extends AbstractMaterializedView<S,E> {

    private MaterializedSubStep<S,?> materializedStartStep;
    private List<MaterializedSubStep<?,?>> materializedSteps;
    private MaterializedSubStep<?,E> materializedEndStep;
    private Graph graph;

    public MaterializedView(String name, Traversal.Admin<S,E> traversal) {
        super(name, traversal);
        graph = traversal.getGraph().get();
        materializedSteps = new ArrayList<>();
        Step s = traversal.asAdmin().getStartStep();
        MaterializedSubStep ms = MaterializedSubStep.of(this, s);
        materializedSteps.add(ms);
        materializedStartStep = ms;
        while (s.getNextStep() != null && !(s.getNextStep() instanceof EmptyStep)) {
            s = s.getNextStep();
            MaterializedSubStep newMs = MaterializedSubStep.of(this, s);
            newMs.setPreviousStep(ms);
            ms.setNextStep(newMs);
            materializedSteps.add(newMs);
            ms = newMs;
        }
        materializedEndStep = ms;
        initialize();
    }

    @Override
    protected void initialize() {
        materializedSteps.forEach(MaterializedSubStep::initialize);
    }

    private Iterator<MaterializedSubStep<?,?>> stepIterator(Delta.Change change) {
        return change == Delta.Change.ADD
                ? new ReverseListIterator(materializedSteps)
                : materializedSteps.iterator();
    }

    @Override
    public void vertexAdded(Vertex vertex) {
        stepIterator(Delta.Change.ADD).forEachRemaining(step -> step.vertexChanged(Delta.add(vertex)));
    }

    @Override
    public void vertexRemoved(Vertex vertex) {
        graph.vertices(vertex).forEachRemaining(v -> v.properties().forEachRemaining(this::vertexPropertyRemoved));
        graph.vertices(vertex).forEachRemaining(v -> v.edges(Direction.BOTH).forEachRemaining(this::edgeRemoved));
        stepIterator(Delta.Change.DEL).forEachRemaining(step -> step.vertexChanged(Delta.del(vertex)));
    }


    @Override
    public void vertexPropertyAdded(VertexProperty<?> vertexProperty) {
        stepIterator(Delta.Change.ADD).forEachRemaining(step -> step.vertexPropertyChanged(Delta.add(vertexProperty)));
    }

    @Override
    public void vertexPropertyRemoved(VertexProperty<?> vertexProperty) {
        graph.vertices(vertexProperty.element()).forEachRemaining(v ->
                v.properties(vertexProperty.key()).forEachRemaining(vp ->
                        vp.properties().forEachRemaining(this::vertexPropertyPropertyRemoved)));
        stepIterator(Delta.Change.DEL).forEachRemaining(step -> step.vertexPropertyChanged(Delta.del(vertexProperty)));
    }

    @Override
    public void edgeAdded(Edge edge) {
        stepIterator(Delta.Change.ADD).forEachRemaining(step -> step.edgeChanged(Delta.add(edge)));
    }

    @Override
    public void edgeRemoved(Edge edge) {
        graph.edges(edge).forEachRemaining(e -> e.properties().forEachRemaining(this::edgePropertyRemoved));
        stepIterator(Delta.Change.DEL).forEachRemaining(step -> step.edgeChanged(Delta.del(edge)));
    }

    @Override
    public void edgePropertyAdded(Property<?> edgeProperty) {
        stepIterator(Delta.Change.ADD).forEachRemaining(step -> step.edgePropertyChanged(Delta.add(edgeProperty)));
    }

    @Override
    public void edgePropertyRemoved(Property<?> edgeProperty) {
        stepIterator(Delta.Change.DEL).forEachRemaining(step -> step.edgePropertyChanged(Delta.del(edgeProperty)));
    }

    @Override
    public void vertexPropertyPropertyAdded(Property<?> vertexPropertyProperty) {
        stepIterator(Delta.Change.ADD).forEachRemaining(step -> step.vertexPropertyPropertyChanged(Delta.add(vertexPropertyProperty)));
    }

    @Override
    public void vertexPropertyPropertyRemoved(Property<?> vertexPropertyProperty) {
        stepIterator(Delta.Change.DEL).forEachRemaining(step -> step.vertexPropertyPropertyChanged(Delta.del(vertexPropertyProperty)));
    }
}
