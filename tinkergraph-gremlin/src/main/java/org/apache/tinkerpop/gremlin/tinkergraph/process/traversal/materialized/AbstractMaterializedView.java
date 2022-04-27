package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractMaterializedView<S,E> implements MutationListener {
    private final String name;
    protected final Traversal.Admin<S,E> baseTraversal;
    protected final List<Traverser.Admin<E>> results;

    public AbstractMaterializedView(String name, Traversal.Admin<S,E> baseTraversal) {
        this.name = name;
        this.baseTraversal = baseTraversal;
        this.results = new ArrayList<>();
    }

    public void registerOutputDelta(Delta<Traverser.Admin<E>> inputChange) {
        if (inputChange.getChange() == Delta.Change.ADD) {
            results.add(inputChange.getObj());
        } else {
            Util.removeFirst(results, t -> t.equals(inputChange.getObj()));
        }
    }

    protected abstract void initialize();

    public String getName() {
        return name;
    }

    public Iterator<Traverser.Admin<E>> iterator() {
        List<Traverser.Admin<E>> clones = new ArrayList<>(results.size());
        for (Traverser.Admin<E> t : results) {
            clones.add(t.clone().asAdmin());
        }
        return clones.iterator();
    }
}
