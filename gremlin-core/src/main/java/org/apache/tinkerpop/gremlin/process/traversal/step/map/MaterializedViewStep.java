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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.materialized.MaterializedView;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public class MaterializedViewStep<S,E> extends AbstractStep<S, E> implements AutoCloseable, Configuring {

    protected Parameters parameters = new Parameters();
    protected MaterializedView<S,E> mView;
    protected boolean done = false;
    private Iterator<Traverser.Admin<E>> iterator;


    public MaterializedViewStep(final Traversal.Admin<S, S> traversal, MaterializedView<S,E> mView) {
        super(traversal);
        this.mView = mView;
        this.iterator = mView.iterator();
    }

    public String toString() {
        return StringFactory.stepString(this, mView.getName());
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(null, keyValues);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
        this.iterator = mView.iterator();
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ mView.hashCode();
    }

    /**
     * Attempts to close an underlying iterator if it is of type {@link CloseableIterator}. Graph providers may choose
     * to return this interface containing their vertices and edges if there are expensive resources that might need to
     * be released at some point.
     */
    @Override
    public void close() {
        CloseableIterator.closeIterator(iterator);
    }
}
