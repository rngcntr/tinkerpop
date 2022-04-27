package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized.step.MaterializedViewStep;

public class MaterializedTraversalSource extends GraphTraversalSource {
    public MaterializedTraversalSource(Graph graph, TraversalStrategies traversalStrategies) {
        super(graph, traversalStrategies);
        addListeners();
    }

    public MaterializedTraversalSource(Graph graph) {
        super(graph);
        addListeners();
    }

    public MaterializedTraversalSource(RemoteConnection connection) {
        super(connection);
        addListeners();
    }

    private void addListeners() {
        final EventStrategy.Builder strategyBuilder = EventStrategy.build();
        for (AbstractMaterializedView mv : MaterializedTraversalStore.getInstance().getViews()) {
            strategyBuilder.addListener(mv);
        }
        strategies.addStrategies(strategyBuilder.create());
    }

    public <S> GraphTraversal<S,S> view(String name) {
        final GraphTraversalSource clone = this.clone();
        final AbstractMaterializedView<?,?> mView = MaterializedTraversalStore.getInstance().getView(name);
        clone.getBytecode().addStep(MaterializedViewStep.SYMBOL, mView.getName());
        final GraphTraversal.Admin<S, S> traversal = new DefaultGraphTraversal<>(clone);
        return traversal.addStep(new MaterializedViewStep(traversal, mView));
    }
}
