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

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;

import java.util.Optional;

public class MaterializedMeanStep<T extends Number> extends MaterializedReducingBarrierStep<T, T, MeanGlobalStep.MeanNumber> {

    public MaterializedMeanStep(AbstractMaterializedView<?,?> mv, MeanGlobalStep<T, T> originalStep) {
        super(mv, originalStep);
    }

    @Override
    protected Optional<MeanGlobalStep.MeanNumber> getSeed() {
        return Optional.empty();
    }

    @Override
    protected T mapState(MeanGlobalStep.MeanNumber state) {
        return (T) state.getFinal();
    }

    @Override
    protected Optional<MeanGlobalStep.MeanNumber> apply(Optional<MeanGlobalStep.MeanNumber> state, Delta<Traverser.Admin<T>> inputChange) {
        if (!previousStep.outputs().hasNext()) {
            return Optional.empty();
        }
        long bulk = inputChange.getObj().bulk();
        return (state.isPresent() ? state : Optional.of(new MeanGlobalStep.MeanNumber())).map(mn ->
                mn.add(inputChange.getObj().get(), inputChange.isAddition() ? bulk : -1 * bulk));
    }
}
