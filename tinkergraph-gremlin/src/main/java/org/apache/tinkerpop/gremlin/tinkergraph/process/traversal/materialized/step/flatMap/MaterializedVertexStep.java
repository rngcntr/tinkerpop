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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

public class MaterializedVertexStep<E extends Element> extends MaterializedFlatMapStep<Vertex,E> {
    private final Direction direction;
    private final Class<E> outputType;

    public MaterializedVertexStep(AbstractMaterializedView<?,?> mv, VertexStep<E> originalStep) {
        super(mv, originalStep);
        this.outputType = originalStep.getReturnClass();
        this.direction = originalStep.getDirection();
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<Vertex>> inputChange) {
        if (inputChange.isDeletion()) {
            return; // deletions of vertices will also cause deletions of edges which we can detect in elementChanged
        }
        final Traverser.Admin<Vertex> t = inputChange.getObj();
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

    @Override
    protected void elementChanged(Delta<? extends Element> delta) {
        if (!(delta.getObj() instanceof Edge)) {
            return;
        }

        if (Edge.class.isAssignableFrom(outputType)) {
            process((Delta<Edge>) delta);
        } else if (Vertex.class.isAssignableFrom(outputType)) {
            process((Delta<Edge>) delta);
        }
    }

    private void process(Delta<Edge> delta) {
        previousStep.outputs().forEachRemaining(t -> {
            if ((direction == Direction.IN | direction == Direction.BOTH) && t.get().id() == delta.getObj().inVertex().id()) {
                if (Vertex.class.isAssignableFrom(outputType)) {
                    deltaOutput(new Delta(delta.getChange(), t.split((E) delta.getObj().outVertex(), originalStep)));
                } else {
                    deltaOutput(new Delta(delta.getChange(), t.split((E) delta.getObj(), originalStep)));
                }
            }
            if ((direction == Direction.OUT | direction == Direction.BOTH) && t.get().id() == delta.getObj().outVertex().id()) {
                if (Vertex.class.isAssignableFrom(outputType)) {
                    deltaOutput(new Delta(delta.getChange(), t.split((E) delta.getObj().inVertex(), originalStep)));
                } else {
                    deltaOutput(new Delta(delta.getChange(), t.split((E) delta.getObj(), originalStep)));
                }
            }
        });
    }
}
