package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.reduce;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Util;

import java.util.Iterator;
import java.util.Optional;

public class MaterializedMaxStep<T extends Comparable<T>> extends MaterializedReducingBarrierStep<T, T, T> {

    public MaterializedMaxStep(AbstractMaterializedView<?,?> mv, MaxGlobalStep<T> originalStep) {
        super(mv, originalStep);
    }

    @Override
    protected Optional<T> getSeed() {
        return Optional.empty();
    }

    @Override
    protected T mapState(T state) {
        return state;
    }

    @Override
    protected Optional<T> apply(Optional<T> state, Delta<Traverser.Admin<T>> inputChange) {
        T obj = inputChange.getObj().get();
        if (state.isPresent()) {
            T s = state.get();
            if (inputChange.getChange() == Delta.Change.ADD) {
                return Optional.of(Util.max(s, obj));
            } else {
                if (s.compareTo(obj) > 0) {
                    return state;
                } else {
                    final Iterator<T> it = Util.mappingIterator(previousStep.outputs(), Traverser::get);
                    return Util.max(() -> it);
                }
            }
        } else {
            if (inputChange.getChange() == Delta.Change.ADD) {
                return Optional.of(obj);
            } else {
                return state;
            }
        }
    }
}
