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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.MaterializedSubStep;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.util.TraverserRequirementStep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.Util.stepIterator;

public class MaterializedView<S,E> extends AbstractMaterializedView<S,E> {

    private MaterializedSubStep<S,?> materializedStartStep;
    private List<MaterializedSubStep<?,?>> materializedSteps;
    private MaterializedSubStep<?,E> materializedEndStep;
    private Graph graph;

    public MaterializedView(String name, Traversal.Admin<S,E> traversal) {
        super(name, traversal);
        graph = traversal.getGraph().get();
        materializedSteps = new ArrayList<>();
        Step s = traversal.asAdmin().getStartStep();
        MaterializedSubStep ms = MaterializedSubStep.of(this, s);
        materializedSteps.add(ms);
        materializedStartStep = ms;
        while (s.getNextStep() != null && !(s.getNextStep() instanceof EmptyStep)) {
            s = s.getNextStep();
            MaterializedSubStep newMs = MaterializedSubStep.of(this, s);
            newMs.setPreviousStep(ms);
            ms.setNextStep(newMs);
            materializedSteps.add(newMs);
            ms = newMs;
        }
        materializedEndStep = ms;

        TraverserRequirementStep trs = new TraverserRequirementStep(traversal, Collections.singleton(TraverserRequirement.SACK));
        traversal.addStep(trs);
        initialize();
    }

    @Override
    protected void initialize() {
        materializedSteps.forEach(MaterializedSubStep::initialize);
    }

    @Override
    public void vertexAdded(Vertex vertex) {
        stepIterator(materializedSteps).forEachRemaining(step -> step.vertexChanged(Delta.add(vertex)));
    }

    @Override
    public void vertexRemoved(Vertex vertex) {
        graph.vertices(vertex).forEachRemaining(v -> v.properties().forEachRemaining(this::vertexPropertyRemoved));
        graph.vertices(vertex).forEachRemaining(v -> v.edges(Direction.BOTH).forEachRemaining(this::edgeRemoved));
        stepIterator(materializedSteps).forEachRemaining(step -> step.vertexChanged(Delta.del(vertex)));
    }


    @Override
    public void vertexPropertyAdded(VertexProperty<?> vertexProperty) {
        stepIterator(materializedSteps).forEachRemaining(step -> step.vertexPropertyChanged(Delta.add(vertexProperty)));
    }

    @Override
    public void vertexPropertyRemoved(VertexProperty<?> vertexProperty) {
        graph.vertices(vertexProperty.element()).forEachRemaining(v ->
                v.properties(vertexProperty.key()).forEachRemaining(vp ->
                        vp.properties().forEachRemaining(this::vertexPropertyPropertyRemoved)));
        stepIterator(materializedSteps).forEachRemaining(step -> step.vertexPropertyChanged(Delta.del(vertexProperty)));
    }

    @Override
    public void edgeAdded(Edge edge) {
        stepIterator(materializedSteps).forEachRemaining(step -> step.edgeChanged(Delta.add(edge)));
    }

    @Override
    public void edgeRemoved(Edge edge) {
        graph.edges(edge).forEachRemaining(e -> e.properties().forEachRemaining(this::edgePropertyRemoved));
        stepIterator(materializedSteps).forEachRemaining(step -> step.edgeChanged(Delta.del(edge)));
    }

    @Override
    public void edgePropertyAdded(Property<?> edgeProperty) {
        stepIterator(materializedSteps).forEachRemaining(step -> step.edgePropertyChanged(Delta.add(edgeProperty)));
    }

    @Override
    public void edgePropertyRemoved(Property<?> edgeProperty) {
        stepIterator(materializedSteps).forEachRemaining(step -> step.edgePropertyChanged(Delta.del(edgeProperty)));
    }

    @Override
    public void vertexPropertyPropertyAdded(Property<?> vertexPropertyProperty) {
        stepIterator(materializedSteps).forEachRemaining(step -> step.vertexPropertyPropertyChanged(Delta.add(vertexPropertyProperty)));
    }

    @Override
    public void vertexPropertyPropertyRemoved(Property<?> vertexPropertyProperty) {
        stepIterator(materializedSteps).forEachRemaining(step -> step.vertexPropertyPropertyChanged(Delta.del(vertexPropertyProperty)));
    }
}
