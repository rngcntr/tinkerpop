package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

import java.util.Optional;

public class MaterializedMeanStep<T extends Number> extends MaterializedReducingBarrierStep<T, T, MeanGlobalStep.MeanNumber> {

    public MaterializedMeanStep(AbstractMaterializedView<?,?> mv, MeanGlobalStep<T, T> originalStep) {
        super(mv, originalStep);
    }

    @Override
    protected Optional<MeanGlobalStep.MeanNumber> getSeed() {
        return Optional.empty();
    }

    @Override
    protected T mapState(MeanGlobalStep.MeanNumber state) {
        return (T) state.getFinal();
    }

    @Override
    protected Optional<MeanGlobalStep.MeanNumber> apply(Optional<MeanGlobalStep.MeanNumber> state, Delta<Traverser.Admin<T>> inputChange) {
        if (!previousStep.outputs().hasNext()) {
            return Optional.empty();
        }
        long bulk = inputChange.getObj().bulk();
        return (state.isPresent() ? state : Optional.of(new MeanGlobalStep.MeanNumber())).map(mn ->
                mn.add(inputChange.getObj().get(), inputChange.getChange() == Delta.Change.ADD ? bulk : -1 * bulk));
    }
}
