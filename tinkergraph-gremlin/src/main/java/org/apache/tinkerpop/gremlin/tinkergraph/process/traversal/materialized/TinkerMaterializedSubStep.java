package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.MaterializedView;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class TinkerMaterializedSubStep<S, E> implements MutationListener {
    private MaterializedView materializedView;
    private Step<S,E> originalStep;
    private TinkerMaterializedSubStep<?,S> previousStep;
    private TinkerMaterializedSubStep<E,?> nextStep;
    private final List<Traverser.Admin<E>> outputs;

    protected TinkerMaterializedSubStep(MaterializedView mv, Step<S,E> originalStep) {
        this.materializedView = mv;
        this.originalStep = originalStep;
        this.outputs = new ArrayList<>();
    }

    public static <S,E> TinkerMaterializedSubStep<S,E> of(MaterializedView mv, Step<S,E> originalStep) {
        if (originalStep instanceof GraphStep) {
            return new TinkerMaterializedGraphStep(mv, (GraphStep) originalStep);
        } else if (originalStep instanceof IdentityStep) {
            return new TinkerMaterializedIdentityStep(mv, originalStep);
        } else if (originalStep instanceof CountGlobalStep) {
            return new TinkerMaterializedCountStep(mv, originalStep);
        } else {
            throw new IllegalArgumentException("No such materializable step");
        }
    }

    public abstract void registerInputDelta(Delta<Traverser.Admin<S>> inputChange);

    protected void deltaOutput(Delta<Traverser.Admin<E>> outputDelta) {
        if (outputDelta.getChange() == Delta.Change.ADD) {
            outputs.add(outputDelta.getObj());
        } else {
            outputs.remove(outputDelta.getObj());
        }
        if (nextStep != null) {
            nextStep.registerInputDelta(outputDelta);
        } else if (materializedView != null) {
            materializedView.registerOutputDelta(outputDelta);
        }
    }

    public Iterator<Traverser.Admin<E>> outputs() {
        return outputs.iterator();
    }

    public void setPreviousStep(TinkerMaterializedSubStep<?, S> previousStep) {
        this.previousStep = previousStep;
    }

    public void setNextStep(TinkerMaterializedSubStep<E, ?> nextStep) {
        this.nextStep = nextStep;
    }

    public TinkerMaterializedSubStep<?,S> getPreviousStep() {
        return previousStep;
    }

    public TinkerMaterializedSubStep<E,?> getNextStep() {
        return nextStep;
    }
}
