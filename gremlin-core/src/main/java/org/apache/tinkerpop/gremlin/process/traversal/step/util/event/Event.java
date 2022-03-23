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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.function.BiConsumer;

/**
 * A representation of some action that occurs on a {@link Graph} for a {@link Traversal}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Event {

    /**
     * An {@code Event} publishes its action to all the event {@link MutationListener} objects.
     */

    void fireEvent(final Iterator<MutationListener> eventListeners);

    /**
     * Template for the implementation of events.
     */
    abstract class AbstractEvent<E> implements Event {
        protected final E element;
        private final BiConsumer<MutationListener, E> eventCall;

        public AbstractEvent(final E element, final BiConsumer<MutationListener, E> eventCall) {
            this.element = element;
            this.eventCall = eventCall;
        }

        @Override
        public void fireEvent(final Iterator<MutationListener> eventListeners) {
            eventListeners.forEachRemaining(listener -> eventCall.accept(listener, element));
        }
    }

    /**
     * Represents an action where an {@link Edge} is added to the {@link Graph}.
     */
    class EdgeAddedEvent extends AbstractEvent<Edge> {
        public EdgeAddedEvent(final Edge edge) {
            super(edge, MutationListener::edgeAdded);
        }
    }

    /**
     * Represents an action where an {@link Edge} is removed from the {@link Graph}.
     */
    class EdgeRemovedEvent extends AbstractEvent<Edge> {
        public EdgeRemovedEvent(final Edge edge) {
            super(edge, MutationListener::edgeRemoved);
        }
    }

    /**
     * Represents an action where a {@link Vertex} is added to the {@link Graph}.
     */
    class VertexAddedEvent extends AbstractEvent<Vertex> {
        public VertexAddedEvent(final Vertex vertex) {
            super(vertex, MutationListener::vertexAdded);
        }
    }

    /**
     * Represents an action where a {@link Vertex} is removed from the {@link Graph}.
     */
    class VertexRemovedEvent extends AbstractEvent<Vertex> {
        public VertexRemovedEvent(final Vertex vertex) {
            super(vertex, MutationListener::vertexRemoved);
        }
    }

    /**
     * Represents an action where a {@link VertexProperty} is added to a {@link Vertex}.
     */
    class VertexPropertyAddedEvent extends AbstractEvent<VertexProperty<?>> {
        public VertexPropertyAddedEvent(final VertexProperty<?> vertexProperty) {
            super(vertexProperty, MutationListener::vertexPropertyAdded);
        }
    }

    /**
     * Represents an action where a {@link VertexProperty} is removed from a {@link Vertex}.
     */
    class VertexPropertyRemovedEvent extends AbstractEvent<VertexProperty<?>> {
        public VertexPropertyRemovedEvent(final VertexProperty<?> vertexProperty) {
            super(vertexProperty, MutationListener::vertexPropertyRemoved);
        }
    }

    /**
     * Represents an action where a {@link Property} is added to an {@link Edge}.
     */
    class EdgePropertyAddedEvent extends AbstractEvent<Property<?>> {
        public EdgePropertyAddedEvent(final Property<?> edgeProperty) {
            super(edgeProperty, MutationListener::edgePropertyAdded);
        }
    }

    /**
     * Represents an action where a {@link Property} is removed from an {@link Edge}.
     */
    class EdgePropertyRemovedEvent extends AbstractEvent<Property<?>> {
        public EdgePropertyRemovedEvent(final Property<?> edgeProperty) {
            super(edgeProperty, MutationListener::edgePropertyRemoved);
        }
    }

    /**
     * Represents an action where a {@link Property} is added to a {@link VertexProperty}.
     */
    class VertexPropertyPropertyAddedEvent extends AbstractEvent<Property<?>> {
        public VertexPropertyPropertyAddedEvent(final Property<?> vertexPropertyProperty) {
            super(vertexPropertyProperty, MutationListener::vertexPropertyPropertyAdded);
        }
    }

    /**
     * Represents an action where a {@link Property} is removed from a {@link VertexProperty}.
     */
    class VertexPropertyPropertyRemovedEvent extends AbstractEvent<Property<?>> {
        public VertexPropertyPropertyRemovedEvent(final Property<?> vertexPropertyProperty) {
            super(vertexPropertyProperty, MutationListener::vertexPropertyPropertyRemoved);
        }
    }
}
