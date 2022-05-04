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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.reduce;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util.MaterializedSubStep;

import java.util.Optional;

public abstract class MaterializedReducingBarrierStep<S, T, U> extends MaterializedSubStep<S,T> {
    private Optional<U> state;
    private Optional<Traverser.Admin<T>> lastOutput;
    final TraverserGenerator generator;

    public MaterializedReducingBarrierStep(AbstractMaterializedView<?,?> mv, ReducingBarrierStep<S,T> originalStep) {
        super(mv, originalStep);
        state = getSeed();
        generator = originalStep.getTraversal().getTraverserGenerator();
        lastOutput = state.map(s -> generator.generate(mapState(s), (Step<T,T>) originalStep, 1L));
    }

    protected abstract Optional<U> getSeed();

    protected abstract T mapState(U state);

    @Override
    public void initialize() {
        lastOutput.ifPresent(o -> deltaOutput(Delta.add(o)));
    }

    protected abstract Optional<U> apply(Optional<U> state, Delta<Traverser.Admin<S>> inputChange);

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<S>> inputChange) {
        state = apply(state, inputChange);
        lastOutput.ifPresent(o -> deltaOutput(Delta.del(o)));
        lastOutput = state.map(s -> generator.generate(mapState(s), (Step<T,T>) originalStep, 1L));
        lastOutput.ifPresent(o -> deltaOutput(Delta.add(o)));
    }
}
