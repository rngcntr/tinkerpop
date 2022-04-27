package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.NoSuchElementException;

public class FakeEmptyStep<S,E> extends AbstractStep<S,E> {

    private FakeEmptyStep(Traversal.Admin traversal) {
        super(traversal);
    }

    public static <S,E> FakeEmptyStep<S,E> of(Step<S,E> other) {
        return new FakeEmptyStep<>(other.getTraversal());
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw FastNoSuchElementException.instance();
    }
}
