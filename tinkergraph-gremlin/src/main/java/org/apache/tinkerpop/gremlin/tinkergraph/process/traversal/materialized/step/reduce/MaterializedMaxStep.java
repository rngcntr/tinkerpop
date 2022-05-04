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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MaxGlobalStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Util;

import java.util.Iterator;
import java.util.Optional;

public class MaterializedMaxStep<T extends Comparable<T>> extends MaterializedReducingBarrierStep<T, T, T> {

    public MaterializedMaxStep(AbstractMaterializedView<?,?> mv, MaxGlobalStep<T> originalStep) {
        super(mv, originalStep);
    }

    @Override
    protected Optional<T> getSeed() {
        return Optional.empty();
    }

    @Override
    protected T mapState(T state) {
        return state;
    }

    @Override
    protected Optional<T> apply(Optional<T> state, Delta<Traverser.Admin<T>> inputChange) {
        T obj = inputChange.getObj().get();
        if (state.isPresent()) {
            T s = state.get();
            if (inputChange.getChange() == Delta.Change.ADD) {
                return Optional.of(Util.max(s, obj));
            } else {
                if (s.compareTo(obj) > 0) {
                    return state;
                } else {
                    final Iterator<T> it = Util.mappingIterator(previousStep.outputs(), Traverser::get);
                    return Util.max(() -> it);
                }
            }
        } else {
            if (inputChange.getChange() == Delta.Change.ADD) {
                return Optional.of(obj);
            } else {
                return state;
            }
        }
    }
}
