/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.local;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.HashMultiSet;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util.MaterializedSubStep;

import java.util.*;

import static org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Util.stepIterator;

public class MaterializedLocalFilterStep<S> extends MaterializedSubStep<S,S> {

    private HashMultiSet<Traverser.Admin<S>> localResultSet;
    private List<MaterializedSubStep<?,?>> localSteps;
    private MaterializedSubStep localStartStep;

    private static final List<Class<? extends FilterStep>> SUPPORTED_STEPS = Arrays.asList(
            NotStep.class, TraversalFilterStep.class
    );

    private final boolean inverseFilter;

    public MaterializedLocalFilterStep(AbstractMaterializedView<?, ?> mv, Step<S, S> originalStep) {
        super(mv, originalStep);
        // TODO: neither parent nor child traversal must not contain steps which use or drop sack
        if (!supports(originalStep) || !(originalStep instanceof TraversalParent)) {
            throw new IllegalArgumentException("Step is not compatible: " + originalStep);
        }
        final List<Traversal.Admin<Object, Object>> localChildren = ((TraversalParent) originalStep).getLocalChildren();
        if (localChildren.size() != 1) {
            throw new IllegalArgumentException("Step is not compatible: " + originalStep);
        }
        inverseFilter = originalStep instanceof NotStep;
        buildLocalTraversal(localChildren.get(0));
        localResultSet = new HashMultiSet<>();
    }

    private void buildLocalTraversal(Traversal.Admin<?,?> localTraversal) {
        localSteps = new ArrayList<>();
        Step<?,?> s = localTraversal.getStartStep();
        MaterializedSubStep<?,?> ms = MaterializedSubStep.of(materializedView, s);
        localSteps.add(ms);
        localStartStep = ms;
        while (s.getNextStep() != null && !(s.getNextStep() instanceof EmptyStep)) {
            s = s.getNextStep();
            MaterializedSubStep newMs = MaterializedSubStep.of(materializedView, s);
            newMs.setPreviousStep(ms);
            ms.setNextStep(newMs);
            localSteps.add(newMs);
            ms = newMs;
        }
        ms.addOutputListener(this::registerLocalDelta);
        initialize();
    }

    @Override
    public void initialize() {
        localSteps.forEach(MaterializedSubStep::initialize);
    }

    @Override
    public void vertexChanged(Delta<Vertex> delta) {
        stepIterator(localSteps, delta.getChange()).forEachRemaining(s -> s.vertexChanged(delta));
    }

    @Override
    public void edgeChanged(Delta<Edge> delta) {
        stepIterator(localSteps, delta.getChange()).forEachRemaining(s -> s.edgeChanged(delta));
    }

    @Override
    public void vertexPropertyChanged(Delta<VertexProperty<?>> delta) {
        stepIterator(localSteps, delta.getChange()).forEachRemaining(s -> s.vertexPropertyChanged(delta));
    }

    @Override
    public void edgePropertyChanged(Delta<Property<?>> delta) {
        stepIterator(localSteps, delta.getChange()).forEachRemaining(s -> s.edgePropertyChanged(delta));
    }

    @Override
    public void vertexPropertyPropertyChanged(Delta<Property<?>> delta) {
        stepIterator(localSteps, delta.getChange()).forEachRemaining(s -> s.vertexPropertyPropertyChanged(delta));
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<S>> inputChange) {
        Traverser.Admin<S> original = inputChange.getObj();
        if (inverseFilter) {
            /*
                for not(...) traversals, the result is added to the set by default and gets removed by
                the registerLocalDelta method
                TODO: Not yet determined if this works with multiple deletions and additions
            */
            if (inputChange.isAddition()) {
                localResultSet.add(original);
                deltaOutput(inputChange);
                processLocal(inputChange);
            } else if (localResultSet.contains(original)){
                processLocal(inputChange);
                localResultSet.remove(original);
                deltaOutput(inputChange);
            }
        } else {
            processLocal(inputChange);
        }
    }

    private void processLocal(Delta<Traverser.Admin<S>> inputChange) {
        Traverser.Admin<S> original = inputChange.getObj();
        Traverser.Admin<S> clone = (Traverser.Admin<S>) original.clone();
        localStartStep.registerInputDelta(inputChange.map(t -> {
            DefaultTraversalSideEffects se = new DefaultTraversalSideEffects();
            // TODO: Merging into null is not safe, but ensures that traverser objects can 'equal' each other
            se.setSack(() -> original, sAdmin -> sAdmin, (sAdmin, sAdmin2) -> null);
            clone.setSideEffects(se);
            clone.sack(original);
            return clone;
        }));
    }

    public <T> void registerLocalDelta(Delta<Traverser.Admin<T>> inputChange) {
        Traverser.Admin<S> sack = inputChange.getObj().sack();

        boolean foundBefore = localResultSet.contains(sack);

        if (inverseFilter && inputChange.isDeletion() || !inverseFilter && inputChange.isAddition()) {
            localResultSet.add(sack);
        } else {
            localResultSet.remove(sack);
        }

        boolean foundAfter = localResultSet.contains(sack);

        if (foundBefore != foundAfter) {
            if (inverseFilter) {
                deltaOutput(inputChange.invert().map(t -> sack));
            } else {
                deltaOutput(inputChange.map(t -> sack));
            }
        }
    }

    public static boolean supports(Step<?,?> step) {
        return SUPPORTED_STEPS.contains(step.getClass());
    }
}
