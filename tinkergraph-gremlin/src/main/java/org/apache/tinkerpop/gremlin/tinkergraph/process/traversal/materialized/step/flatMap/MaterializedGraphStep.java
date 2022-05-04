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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

public class MaterializedGraphStep<S, E extends Element> extends MaterializedFlatMapStep<S,E> {
    Class<E> outputType;

    public MaterializedGraphStep(AbstractMaterializedView<?,?> mv, GraphStep<S,E> originalStep) {
        super(mv, originalStep);
        this.outputType = originalStep.getReturnClass();
        if (previousStep == null) {
            clonedStep.setPreviousStep(EmptyStep.instance());
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

    @Override
    protected void elementChanged(Delta<? extends Element> delta) {
        if (outputType.isAssignableFrom(delta.getObj().getClass())) {
            if (previousStep == null) {
                processSingleClonedStep(delta);
            } else {
                processAllClonedStep(delta);
            }
        }
    }
}
