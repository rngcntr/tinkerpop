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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.flatMap;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

import java.util.Arrays;
import java.util.List;

public class StatefulMaterializedFlatMapStep<S,E extends Element> extends MaterializedFlatMapStep<S,E> {

    private static final List<Class<? extends FlatMapStep>> SUPPORTED_STEPS = Arrays.asList(
            EdgeVertexStep.class
    );

    public static boolean supports(Step<?,?> step) {
        return SUPPORTED_STEPS.contains(step.getClass());
    }

    public StatefulMaterializedFlatMapStep(AbstractMaterializedView mv, Step<S, E> originalStep) {
        super(mv, originalStep);
        if (!supports(originalStep)) {
            throw new IllegalArgumentException("Step is not compatible: " + originalStep);
        }
    }

    @Override
    protected void elementChanged(Delta<? extends Element> delta) {
        if (delta.getObj() instanceof Edge) {
            processSingleClonedStep(delta);
        }
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<S>> inputChange) {
        final Traverser.Admin<S> t = inputChange.getObj();
        t.attach(Attachable.Method.get(graph));
        clonedStep.addStart(t);
        while (clonedStep.hasNext()) {
            Traverser.Admin<E> tOut = clonedStep.next();
            tOut.detach();
            deltaOutput(inputChange.map(o -> tOut));
        }
        t.detach();
        clonedStep.reset();
    }
}
