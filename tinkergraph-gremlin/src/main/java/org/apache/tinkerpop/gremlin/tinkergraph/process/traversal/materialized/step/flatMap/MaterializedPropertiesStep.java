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

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.MaterializedSubStep;

public class MaterializedPropertiesStep<E> extends MaterializedSubStep<Element,E> {

    public MaterializedPropertiesStep(AbstractMaterializedView<?,?> mv, PropertiesStep<E> originalStep) {
        super(mv, originalStep);
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<Element>> inputChange) {
        if (inputChange.isDeletion()) {
            return; // deletions of vertices will also cause deletions of properties which we can detect in elementChanged
        }
        final Traverser.Admin<Element> t = inputChange.getObj();
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

    /*
     * According to <code>change</code> either adds or removes the
     * <code>clonedStep</code>'s outputs which match <code>element</code>
     */
    private void processSingleClonedStep(Delta<? extends Property<?>> delta) {
        while (clonedStep.hasNext()) {
            Traverser.Admin<E> tOut = clonedStep.next();
            tOut.detach();
            if (((Property<?>) tOut.get()).element().id() == delta.getObj().element().id()
                    && tOut.get().equals(delta.getObj())) {
                deltaOutput(delta.map(ignored -> tOut));
            }
        }
        clonedStep.reset();
    }

    private void processAllClonedStep(Delta<? extends Property<?>> delta) {
        previousStep.outputs().forEachRemaining(t -> {
            t.attach(Attachable.Method.get(graph));
            clonedStep.addStart(t);
            processSingleClonedStep(delta);
            t.detach();
        });
        clonedStep.reset();
    }

    @Override
    public void vertexPropertyChanged(Delta<VertexProperty<?>> delta) {
        elementChanged(delta);
    }

    @Override
    public void edgePropertyChanged(Delta<Property<?>> delta) {
        elementChanged(delta);
    }

    @Override
    public void vertexPropertyPropertyChanged(Delta<Property<?>> delta) {
        elementChanged(delta);
    }

    private void elementChanged(Delta<? extends Property<?>> delta) {
        processAllClonedStep(delta);
    }
}
