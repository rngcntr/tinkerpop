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

import org.apache.commons.collections.iterators.ReverseListIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public class Util {
    public static <E> void removeFirst(List<Traverser.Admin<E>> outputs, Predicate<Traverser.Admin<E>> predicate) {
        for (int i = 0; i < outputs.size(); i++) {
            if (predicate.test(outputs.get(i))) {
                outputs.remove(i);
                break;
            }
        }
    }

    public static <A,B> Iterator<B> mappingIterator(Iterator<A> it, Function<A,B> mapper) {
        return new Iterator<B>() {
            @Override
            public boolean hasNext() { return it.hasNext(); }

            @Override
            public B next() { return mapper.apply(it.next()); }
        };
    }

    public static <E extends Comparable<E>> E min(E a, E b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    public static <E extends Comparable<E>> E max(E a, E b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    private static <E extends Comparable<E>> Optional<E> reduce(Iterable<E> iterable, BiFunction<E,E,E> op) {
        final Iterator<E> it = iterable.iterator();
        if (!it.hasNext()) {
            return Optional.empty();
        } else {
            E state = it.next();
            while (it.hasNext()) {
                final E next = it.next();
                state = op.apply(state, next);
            }
            return Optional.of(state);
        }
    }

    public static <E extends Comparable<E>> Optional<E> max(Iterable<E> iterable) {
        return reduce(iterable, Util::max);
    }

    public static <E extends Comparable<E>> Optional<E> min(Iterable<E> iterable) {
        return reduce(iterable, Util::min);
    }

    public static <E> Iterator<E> stepIterator(List<E> list, Delta.Change change) {
        return change == Delta.Change.ADD
                ? new ReverseListIterator(list)
                : list.iterator();
    }

    public static Graph getGraph(Step<?,?> step) {
        Traversal.Admin<?,?> traversal = step.getTraversal();
        while (!traversal.getGraph().isPresent() || traversal.getGraph().get() == EmptyGraph.instance()) {
            final TraversalParent parent = traversal.getParent();
            if (parent == EmptyStep.instance()) {
                return null;
            }
            traversal = parent.asStep().getTraversal();
        }
        return traversal.getGraph().get();
    }
}
