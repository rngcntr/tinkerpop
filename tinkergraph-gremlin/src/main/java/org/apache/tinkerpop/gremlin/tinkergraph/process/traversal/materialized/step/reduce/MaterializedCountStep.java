package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

import java.util.Optional;

public class MaterializedCountStep<S> extends MaterializedReducingBarrierStep<S, Long, Long> {

    public MaterializedCountStep(AbstractMaterializedView<?,?> mv, CountGlobalStep<S> originalStep) {
        super(mv, originalStep);
    }

    @Override
    protected Optional<Long> getSeed() {
        return Optional.of(0L);
    }

    @Override
    protected Long mapState(Long state) {
        return state;
    }

    @Override
    protected Optional<Long> apply(Optional<Long> state, Delta<Traverser.Admin<S>> inputChange) {
        final long bulk = inputChange.getObj().bulk();
        return state.map(s -> s + (inputChange.getChange() == Delta.Change.ADD ? bulk : -1 * bulk));
    }
}
