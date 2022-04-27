package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.flatMap;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

public class MaterializedGraphStep<S, E extends Element> extends MaterializedFlatMapStep<S,E> {
    Class<E> outputType;

    public MaterializedGraphStep(AbstractMaterializedView<?,?> mv, GraphStep<S,E> originalStep) {
        super(mv, originalStep);
        this.outputType = originalStep.getReturnClass();
        if (previousStep == null) {
            clonedStep.setPreviousStep(EmptyStep.instance());
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

    @Override
    protected void elementChanged(Delta<? extends Element> delta) {
        if (outputType.isAssignableFrom(delta.getObj().getClass())) {
            if (previousStep == null) {
                processSingleClonedStep(delta);
            } else {
                processAllClonedStep(delta);
            }
        }
    }
}
