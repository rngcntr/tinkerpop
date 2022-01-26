package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class TinkerMaterializedSubStep<S, E> implements MutationListener {
    private TinkerMaterializedSubStep<?,S> previousStep;
    private TinkerMaterializedSubStep<E,?> nextStep;
    private final List<Traverser.Admin<E>> outputs;

    public static <S,E> TinkerMaterializedSubStep<S,E> of(Step<S,E> originalStep) {
        if (originalStep instanceof GraphStep) {
            return new TinkerMaterializedGraphStep((GraphStep) originalStep);
        } else if (originalStep instanceof IdentityStep) {
            return new TinkerMaterializedIdentityStep();
        } else if (originalStep instanceof CountGlobalStep) {
            return new TinkerMaterializedCountStep(originalStep);
        } else {
            throw new IllegalArgumentException("No such materializable step");
        }
    }

    protected TinkerMaterializedSubStep() {
        outputs = new ArrayList<>();
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
