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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.MaterializedSubStep;

import java.util.Arrays;
import java.util.List;

public class StatelessMaterializedMapStep<S,T> extends MaterializedSubStep<S,T> {

    private static final List<Class<? extends Step>> SUPPORTED_STEPS = Arrays.asList(
            LabelStep.class,
            PropertyKeyStep.class,
            PropertyValueStep.class,
            ConstantStep.class,
            IdentityStep.class,
            LoopsStep.class,
            SackStep.class,
            PathStep.class,
            EdgeOtherVertexStep.class,
            SelectOneStep.class
    );

    public StatelessMaterializedMapStep(AbstractMaterializedView<?,?> mv, Step<S,T> originalStep) {
        super(mv, originalStep);
        if (!supports(originalStep)) {
            throw new IllegalArgumentException("Step is not compatible: " + originalStep);
        }
    }

    @Override
    public void registerInputDelta(Delta<Traverser.Admin<S>> inputChange) {
        clonedStep.addStart(inputChange.getObj());
        deltaOutput(inputChange.map(t -> clonedStep.next()));
        clonedStep.reset();
    }

    public static boolean supports(Step<?,?> step) {
        return SUPPORTED_STEPS.contains(step.getClass());
    }
}
