package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.NoSuchElementException;
import java.util.Set;


public final class TraverserRequirementStep<S> extends AbstractStep<S, S> {

    private final Set<TraverserRequirement> traverserRequirements;

    public TraverserRequirementStep(final Traversal.Admin traversal, final Set<TraverserRequirement> traverserRequirements) {
        super(traversal);
        this.traverserRequirements = traverserRequirements;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return traverserRequirements;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        return this.starts.next();
    }
}
