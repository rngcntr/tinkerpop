package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;

import java.util.Iterator;

public class MaterializedViewStep<S,E> extends AbstractStep<S, E> implements AutoCloseable, Configuring {

    public static String SYMBOL = "view";

    protected Parameters parameters = new Parameters();
    protected AbstractMaterializedView<S,E> mView;
    protected boolean done = false;
    private Iterator<Traverser.Admin<E>> iterator;


    public MaterializedViewStep(final Traversal.Admin<S, S> traversal, AbstractMaterializedView<S,E> mView) {
        super(traversal);
        this.mView = mView;
        this.iterator = mView.iterator();
    }

    public String toString() {
        return StringFactory.stepString(this, mView.getName());
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
        this.iterator = mView.iterator();
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ mView.hashCode();
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(iterator);
    }
}
