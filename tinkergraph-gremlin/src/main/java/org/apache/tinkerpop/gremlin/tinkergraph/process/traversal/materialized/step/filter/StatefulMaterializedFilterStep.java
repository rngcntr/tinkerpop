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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.HashMultiMap;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util.MaterializedSubStep;

import java.util.*;

public class StatefulMaterializedFilterStep <S> extends MaterializedSubStep<S,S> {

    private HashMultiMap<Traverser.Admin<S>, Boolean> matchMap;

    private static final List<Class<? extends FilterStep>> SUPPORTED_STEPS = Arrays.asList(
            HasStep.class
    );

    public StatefulMaterializedFilterStep(AbstractMaterializedView<?,?> mv, Step<S, S> originalStep) {
        super(mv, originalStep);
        if (!supports(originalStep)) {
            throw new IllegalArgumentException("Step is not compatible: " + originalStep);
        }
        matchMap = new HashMultiMap<>();
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<S>> inputChange) {
        Traverser.Admin<S> t = inputChange.getObj();
        S elem = t.get();
        if (elem instanceof Element) {
            boolean matchDetected = matchesElement(t);
            if (inputChange.getChange() == Delta.Change.ADD) {
                matchMap.put(t, matchDetected);
            } else {
                matchMap.remove(t);
            }
            if (matchDetected) {
                deltaOutput(inputChange);
            }
        }
    }

    @Override
    public void vertexPropertyChanged(Delta<VertexProperty<?>> delta) {
        reprocessInputs(delta);
    }

    @Override
    public void edgePropertyChanged(Delta<Property<?>> delta) {
        reprocessInputs(delta);
    }

    @Override
    public void vertexPropertyPropertyChanged(Delta<Property<?>> delta) {
        reprocessInputs(delta);
    }

    public void reprocessInputs(Delta<? extends Property<?>> delta) {
        Map<Traverser.Admin<S>, Boolean> newMatchMap = new HashMap<>();
        Element propertyHost = delta.getObj().element();
        for (Iterator<Traverser.Admin<S>> it = previousStep.outputs(); it.hasNext(); ) {
            Traverser.Admin<S> t = it.next();
            if (t.get() instanceof Element && ((Element) t.get()).id() == propertyHost.id()) {
                newMatchMap.put(t, matchesElement(t));
                if (!matchMap.get(t) && newMatchMap.get(t)) {
                    deltaOutput(Delta.add(t));
                } else if (matchMap.get(t) && !newMatchMap.get(t)) {
                    deltaOutput(Delta.del(t));
                }
            }
        }
        matchMap.putAll(newMatchMap);
    }

    private boolean matchesElement(Traverser.Admin<S> t) {
        t.attach(Attachable.Method.get(graph));
        clonedStep.addStart(t);
        boolean returnValue = clonedStep.hasNext();
        t.detach();
        clonedStep.reset();
        return returnValue;
    }

    public static boolean supports(Step<?,?> step) {
        return SUPPORTED_STEPS.contains(step.getClass());
    }
}
