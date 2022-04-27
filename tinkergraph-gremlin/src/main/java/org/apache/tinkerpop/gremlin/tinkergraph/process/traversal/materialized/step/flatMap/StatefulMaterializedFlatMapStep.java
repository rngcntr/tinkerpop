package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.flatMap;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

import java.util.Arrays;
import java.util.List;

public class StatefulMaterializedFlatMapStep<S,E extends Element> extends MaterializedFlatMapStep<S,E> {

    private static final List<Class<? extends FlatMapStep>> SUPPORTED_STEPS = Arrays.asList(
            EdgeVertexStep.class
    );

    public static boolean supports(Step<?,?> step) {
        return SUPPORTED_STEPS.contains(step.getClass());
    }

    public StatefulMaterializedFlatMapStep(AbstractMaterializedView mv, Step<S, E> originalStep) {
        super(mv, originalStep);
        if (!supports(originalStep)) {
            throw new IllegalArgumentException("Step is not compatible: " + originalStep);
        }
    }

    @Override
    protected void elementChanged(Delta<? extends Element> delta) {
        if (delta.getObj() instanceof Edge) {
            processSingleClonedStep(delta);
        }
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<S>> inputChange) {
        final Traverser.Admin<S> t = inputChange.getObj();
        t.attach(Attachable.Method.get(graph));
        clonedStep.addStart(t);
        while (clonedStep.hasNext()) {
            Traverser.Admin<E> tOut = clonedStep.next();
            tOut.detach();
            deltaOutput(inputChange.map(o -> tOut));
        }
        t.detach();
        clonedStep.reset();
    }
}
