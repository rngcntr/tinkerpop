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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util.MaterializedSubStep;

public abstract class MaterializedFlatMapStep<S, E extends Element> extends MaterializedSubStep<S,E> {

    public MaterializedFlatMapStep(AbstractMaterializedView<?,?> mv, Step<S,E> originalStep) {
        super(mv, originalStep);
    }

    /*
     * According to <code>change</code> either adds or removes the
     * <code>clonedStep</code>'s outputs which match <code>element</code>
     */
    protected void processSingleClonedStep(Delta<? extends Element> delta) {
        while (clonedStep.hasNext()) {
            Traverser.Admin<E> tOut = clonedStep.next();
            tOut.detach();
            if (tOut.get().id() == delta.getObj().id()) {
                deltaOutput(delta.map(ignored -> tOut));
            }
        }
        clonedStep.reset();
    }

    protected void processAllClonedStep(Delta<? extends Element> delta) {
        previousStep.outputs().forEachRemaining(t -> {
            t.attach(Attachable.Method.get(graph));
            clonedStep.addStart(t);
            processSingleClonedStep(delta);
            t.detach();
        });
        clonedStep.reset();
    }

    @Override
    public void vertexChanged(Delta<Vertex> delta) {
        elementChanged(delta);
    }

    @Override
    public void edgeChanged(Delta<Edge> delta) {
        elementChanged(delta);
    }

    protected abstract void elementChanged(Delta<? extends Element> delta);
}
