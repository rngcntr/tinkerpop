package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MaterializedTraversalStore {

    private static MaterializedTraversalStore INSTANCE;

    private final Map<String, AbstractMaterializedView> materializedViews;

    private MaterializedTraversalStore() {
        materializedViews = new ConcurrentHashMap<>();
    }

    public static MaterializedTraversalStore getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new MaterializedTraversalStore();
        }
        return INSTANCE;
    }

    public <S,E> void registerView(AbstractMaterializedView<S,E> mView) {
        materializedViews.put(mView.getName(), mView);
    }

    public AbstractMaterializedView getView(String name) {
        return materializedViews.get(name);
    }

    public Collection<AbstractMaterializedView> getViews() {
        return materializedViews.values();
    }
}
