package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.reduce.MaterializedReducingBarrierStep;
import org.apache.tinkerpop.gremlin.util.NumberHelper;

import java.util.Optional;

public class MaterializedSumStep<T extends Number> extends MaterializedReducingBarrierStep<T, T, T> {

    public MaterializedSumStep(AbstractMaterializedView<?,?> mv, SumGlobalStep<T> originalStep) {
        super(mv, originalStep);
    }

    @Override
    protected Optional<T> getSeed() {
        return Optional.of((T) Byte.valueOf((byte) 0));
    }

    @Override
    protected T mapState(T state) {
        return state;
    }

    @Override
    protected Optional<T> apply(Optional<T> state, Delta<Traverser.Admin<T>> inputChange) {
        if (!previousStep.outputs().hasNext()) {
            return Optional.empty();
        }
        Number changeValue = NumberHelper.mul(inputChange.getObj().get(), inputChange.getObj().bulk());
        Number signedChangeValue = NumberHelper.mul(changeValue, inputChange.getChange() == Delta.Change.ADD ? 1 : -1);
        return (state.isPresent() ? state : getSeed()).map(s -> (T) NumberHelper.add(s, signedChangeValue));
    }
}
