package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.materialized;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

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
}
