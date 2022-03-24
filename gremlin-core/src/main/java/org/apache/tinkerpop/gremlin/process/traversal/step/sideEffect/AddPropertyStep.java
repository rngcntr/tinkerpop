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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventCallback;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.keyed.KeyedProperty;
import org.apache.tinkerpop.gremlin.structure.util.keyed.KeyedVertexProperty;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddPropertyStep<S extends Element> extends SideEffectStep<S>
        implements Mutating<Event>, TraversalParent, Scoping {

    private Parameters parameters = new Parameters();
    private final VertexProperty.Cardinality cardinality;
    private CallbackRegistry<Event> callbackRegistry;

    public AddPropertyStep(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject) {
        super(traversal);
        this.parameters.set(this, T.key, keyObject, T.value, valueObject);
        this.cardinality = cardinality;
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.parameters.getReferencedLabels();
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Object k = this.parameters.get(traverser, T.key, () -> {
            throw new IllegalStateException("The AddPropertyStep does not have a provided key: " + this);
        }).get(0);

        // T identifies immutable components of elements. only property keys can be modified.
        if (k instanceof T)
            throw new IllegalStateException(String.format("T.%s is immutable on existing elements", ((T) k).name()));

        final String key = (String) k;
        final Object value = this.parameters.get(traverser, T.value, () -> {
            throw new IllegalStateException("The AddPropertyStep does not have a provided value: " + this);
        }).get(0);
        final Object[] vertexPropertyKeyValues = this.parameters.getKeyValues(traverser, T.key, T.value);

        final Element element = traverser.get();

        // can't set cardinality if the element is something other than a vertex as only vertices can have
        // a cardinality of properties. if we don't throw an error here we end up with a confusing cast exception
        // which doesn't explain what went wrong
        if (this.cardinality != null && !(element instanceof Vertex))
            throw new IllegalStateException(String.format(
                    "Property cardinality can only be set for a Vertex but the traversal encountered %s for key: %s",
                    element.getClass().getSimpleName(), key));

        VertexProperty.Cardinality cardinality = this.cardinality != null
                ? this.cardinality
                : element.graph().features().vertex().getCardinality(key);

        final Optional<EventStrategy> optEventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class);
        final boolean eventingIsConfigured =  this.callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty()
                && optEventStrategy.isPresent();
        final EventStrategy es = optEventStrategy.orElse(null);

        // only need to capture the removedProperty if eventing is configured
        if (eventingIsConfigured) {
            Property<?> removedProperty = VertexProperty.empty();

            if (element instanceof Vertex) {
                if (cardinality == VertexProperty.Cardinality.set) {
                    final Iterator<? extends Property<?>> properties = element.properties(key);
                    while (properties.hasNext()) {
                        final Property<?> property = properties.next();
                        if (Objects.equals(property.value(), value)) {
                            removedProperty = property;
                            break;
                        }
                    }
                } else if (cardinality == VertexProperty.Cardinality.single) {
                    removedProperty = element.property(key);
                }
            } else {
                removedProperty = element.property(key);
            }

            if (removedProperty.isPresent()) {
                triggerEvents(element, removedProperty, es, true);
            }
        }

        Property<?> updatedProperty = null;

        // update property
        if (element instanceof Vertex) {
            if (null != this.cardinality) {
                updatedProperty = ((Vertex) element).property(this.cardinality, key, value, vertexPropertyKeyValues);
            } else if (vertexPropertyKeyValues.length > 0) {
                updatedProperty = ((Vertex) element).property(key, value, vertexPropertyKeyValues);
            } else {
                updatedProperty = ((Vertex) element).property(key, value);
            }
        } else if (element instanceof Edge) {
            updatedProperty = element.property(key, value);
        } else if (element instanceof VertexProperty) {
            updatedProperty = element.property(key, value);
        }

        if (eventingIsConfigured) {
            triggerEvents(element, updatedProperty, es, false);
        }
    }

    private void triggerEvents(Element element, Property<?> property, EventStrategy es, boolean removed) {
        final Event event;
        if (element instanceof Vertex) {
            event = removed
                    ? new Event.VertexPropertyRemovedEvent(es.detach((VertexProperty<?>) property))
                    : new Event.VertexPropertyAddedEvent(es.detach((VertexProperty<?>) property));
        } else if (element instanceof Edge) {
            event = removed
                    ? new Event.EdgePropertyRemovedEvent(es.detach(property))
                    : new Event.EdgePropertyAddedEvent(es.detach(property));
        } else if (element instanceof VertexProperty) {
            event = removed
                    ? new Event.VertexPropertyPropertyRemovedEvent(es.detach(property))
                    : new Event.VertexPropertyPropertyAddedEvent(es.detach(property));
        } else {
            throw new IllegalStateException(String.format("The incoming object cannot be processed by change eventing in %s:  %s",
                    AddPropertyStep.class.getName(), element));
        }
        this.callbackRegistry.getCallbacks().forEach(c -> c.accept(event));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
    }

    @Override
    public int hashCode() {
        final int hash = super.hashCode() ^ this.parameters.hashCode();
        return (null != this.cardinality) ? (hash ^ cardinality.hashCode()) : hash;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.parameters.getTraversals().forEach(this::integrateChild);
    }

    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.parameters);
    }

    @Override
    public AddPropertyStep<S> clone() {
        final AddPropertyStep<S> clone = (AddPropertyStep<S>) super.clone();
        clone.parameters = this.parameters.clone();
        return clone;
    }
}
