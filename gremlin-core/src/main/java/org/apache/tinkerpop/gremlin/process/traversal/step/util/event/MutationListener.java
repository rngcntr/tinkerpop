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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.event;

import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Interface for a listener to {@link EventStrategy} change events. Implementations of this interface should be added
 * to the list of listeners on the addListener method on the {@link EventStrategy}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MutationListener {

    /**
     * Raised after a {@link Vertex} was added to the graph.
     *
     * @param vertex the {@link Vertex} that was added
     */
    public void vertexAdded(final Vertex vertex);

    /**
     * Raised before a {@link Vertex} is removed from the graph.
     *
     * @param vertex the {@link Vertex} that will be removed.
     */
    public void vertexRemoved(final Vertex vertex);

    /**
     * Raised after a {@link VertexProperty} was added to a {@link Vertex}.
     *
     * @param vertexProperty the {@link VertexProperty} that was added.
     */
    public void vertexPropertyAdded(final VertexProperty<?> vertexProperty);

    /**
     * Raised before a {@link VertexProperty} is removed from a {@link Vertex}.
     *
     * @param vertexProperty the {@link VertexProperty} that will be removed.
     */
    public void vertexPropertyRemoved(final VertexProperty<?> vertexProperty);

    /**
     * Raised after an {@link Edge} was added to the graph.
     *
     * @param edge the {@link Edge} that was added.
     */
    public void edgeAdded(final Edge edge);

    /**
     * Raised before an {@link Edge} is removed from the graph.
     *
     * @param edge  the {@link Edge} that will be removed.
     */
    public void edgeRemoved(final Edge edge);

    /**
     * Raised after a {@link Property} was added to an {@link Edge}.
     *
     * @param edgeProperty the {@link Property} that was added.
     */
    public void edgePropertyAdded(Property<?> edgeProperty);

    /**
     * Raised before a {@link Property} is removed from an {@link Edge}.
     *
     * @param property the {@link Property} that will be removed.
     */
    public void edgePropertyRemoved(final Property<?> property);

    /**
     * Raised after the a {@link Property} was added to a {@link VertexProperty}.
     *
     * @param vertexPropertyProperty the {@link Property} that was added.
     */
    public void vertexPropertyPropertyAdded(Property<?> vertexPropertyProperty);

    /**
     * Raised before a {@link Property} is removed from a {@link VertexProperty}.
     *
     * @param vertexPropertyProperty the {@link Property} that will be removed
     */
    public void vertexPropertyPropertyRemoved(final Property<?> vertexPropertyProperty);
}

