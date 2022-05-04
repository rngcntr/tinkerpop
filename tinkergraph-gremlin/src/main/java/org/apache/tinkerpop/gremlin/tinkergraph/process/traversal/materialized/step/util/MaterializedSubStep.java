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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.AbstractMaterializedView;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Delta;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Util;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.filter.StatefulMaterializedFilterStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.filter.StatelessMaterializedFilterStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.flatMap.MaterializedGraphStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.flatMap.MaterializedPropertiesStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.flatMap.MaterializedVertexStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.flatMap.StatefulMaterializedFlatMapStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.local.MaterializedLocalFilterStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.map.StatelessMaterializedMapStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.reduce.*;

import java.util.*;
import java.util.function.Consumer;

public abstract class MaterializedSubStep<S,E> {
    protected AbstractMaterializedView<?,?> materializedView;
    protected final Graph graph;
    protected final Step<S,E> clonedStep;
    protected final Step<S,E> originalStep;
    protected MaterializedSubStep<?,S> previousStep;
    protected MaterializedSubStep<E,?> nextStep;
    protected final List<Traverser.Admin<E>> outputs;
    private final List<Consumer<Delta<Traverser.Admin<E>>>> outputListeners;

    protected MaterializedSubStep(AbstractMaterializedView<?,?> mv, Step<S,E> originalStep) {
        this.materializedView = mv;
        this.originalStep = originalStep;
        this.graph = originalStep.getTraversal().getGraph().get();
        this.clonedStep = originalStep.clone();
        this.clonedStep.setPreviousStep(FakeEmptyStep.of(originalStep.getPreviousStep()));
        this.clonedStep.setNextStep(FakeEmptyStep.of(originalStep.getNextStep()));
        this.clonedStep.setTraversal(originalStep.getTraversal());
        this.outputs = new ArrayList<>();
        this.outputListeners = new ArrayList<>();
    }

    public static <S,E> MaterializedSubStep<S,E> of(AbstractMaterializedView mv, Step<S,E> originalStep) {
        if (StatelessMaterializedMapStep.supports(originalStep)) {
            return new StatelessMaterializedMapStep(mv, originalStep);
        } else if (StatelessMaterializedFilterStep.supports(originalStep)) {
            return new StatelessMaterializedFilterStep(mv, originalStep);
        } else if (StatefulMaterializedFilterStep.supports(originalStep)) {
            return new StatefulMaterializedFilterStep(mv, originalStep);
        } else if (StatefulMaterializedFlatMapStep.supports(originalStep)) {
            return new StatefulMaterializedFlatMapStep(mv, originalStep);
        } else if (MaterializedLocalFilterStep.supports(originalStep)) {
            return new MaterializedLocalFilterStep(mv, originalStep);
        } else if (originalStep instanceof GraphStep) {
            return new MaterializedGraphStep(mv, (GraphStep) originalStep);
        } else if (originalStep instanceof VertexStep) {
            return new MaterializedVertexStep(mv, (VertexStep) originalStep);
        } else if (originalStep instanceof CountGlobalStep) {
            return new MaterializedCountStep(mv, (CountGlobalStep) originalStep);
        } else if (originalStep instanceof SumGlobalStep) {
            return new MaterializedSumStep(mv, (SumGlobalStep) originalStep);
        } else if (originalStep instanceof MinGlobalStep) {
            return new MaterializedMinStep(mv, (MinGlobalStep) originalStep);
        } else if (originalStep instanceof MaxGlobalStep) {
            return new MaterializedMaxStep(mv, (MaxGlobalStep) originalStep);
        } else if (originalStep instanceof MeanGlobalStep) {
            return new MaterializedMeanStep(mv, (MeanGlobalStep) originalStep);
        } else if (originalStep instanceof PropertiesStep) {
            return new MaterializedPropertiesStep(mv, (PropertiesStep) originalStep);
        } else {
            throw new IllegalArgumentException("No such materializable step: " + originalStep.toString());
        }
    }

    public void initialize() {

    }

    public abstract void registerInputDelta(Delta<Traverser.Admin<S>> inputChange);

    protected void deltaOutput(Delta<Traverser.Admin<E>> outputDelta) {
        if (outputDelta.isAddition()) {
            outputs.add(outputDelta.getObj());
        } else {
            final Traverser.Admin<E> deltaT = outputDelta.getObj();
            Util.removeFirst(outputs, t -> t.equals(deltaT));
        }

        if (nextStep != null) {
            nextStep.registerInputDelta(outputDelta);
        } else if (originalStep.getTraversal().getParent() == EmptyStep.instance() && materializedView != null) {
            materializedView.registerOutputDelta((Delta) outputDelta);
        }

        outputListeners.forEach(l -> l.accept(outputDelta));
    }

    public void addOutputListener(Consumer<Delta<Traverser.Admin<E>>> listener) {
        outputListeners.add(listener);
    }

    public Iterator<Traverser.Admin<E>> outputs() {
        return outputs.iterator();
    }

    public void setPreviousStep(MaterializedSubStep<?, S> previousStep) {
        this.previousStep = previousStep;
    }

    public void setNextStep(MaterializedSubStep<E, ?> nextStep) {
        this.nextStep = nextStep;
    }

    public MaterializedSubStep<?,S> getPreviousStep() {
        return previousStep;
    }

    public MaterializedSubStep<E,?> getNextStep() {
        return nextStep;
    }

    public void vertexChanged(Delta<Vertex> delta) {}

    public void vertexPropertyChanged(Delta<VertexProperty<?>> delta) {}

    public void edgeChanged(Delta<Edge> delta) {}

    public void edgePropertyChanged(Delta<Property<?>> delta) {}

    public void vertexPropertyPropertyChanged(Delta<Property<?>> delta) {}
}
