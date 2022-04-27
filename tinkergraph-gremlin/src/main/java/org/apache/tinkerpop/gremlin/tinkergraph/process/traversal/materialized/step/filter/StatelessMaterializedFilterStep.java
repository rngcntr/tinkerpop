package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.PathFilterStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util.MaterializedSubStep;

import java.util.Arrays;
import java.util.List;

public class StatelessMaterializedFilterStep<S> extends MaterializedSubStep<S,S> {

    private static final List<Class<? extends FilterStep>> SUPPORTED_STEPS = Arrays.asList(
            IsStep.class,
            NoneStep.class,
            PathFilterStep.class
    );

    public StatelessMaterializedFilterStep(AbstractMaterializedView<?,?> mv, Step<S, S> originalStep) {
        super(mv, originalStep);
        if (!supports(originalStep)) {
            throw new IllegalArgumentException("Step is not compatible: " + originalStep);
        }
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<S>> inputChange) {
        clonedStep.addStart(inputChange.getObj());

        if (clonedStep.hasNext()) {
            deltaOutput(inputChange);
        }
        clonedStep.reset();
    }

    public static boolean supports(Step<?,?> step) {
        return SUPPORTED_STEPS.contains(step.getClass());
    }
}
