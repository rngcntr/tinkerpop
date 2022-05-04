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

import java.util.*;

public class HashMultiSet<K> implements Set<K> {
    private HashMap<K,Long> countMap;

    public HashMultiSet() {
        countMap = new HashMap<>();
    }

    @Override
    public int size() {
        return countMap.size();
    }

    @Override
    public boolean isEmpty() {
        return countMap.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return countMap.containsKey(o);
    }

    @Override
    public Iterator<K> iterator() {
        return countMap.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return countMap.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        return countMap.keySet().toArray(ts);
    }

    @Override
    public boolean add(K k) {
        countMap.compute(k, (kk, vv) -> vv == null ? 1 : vv + 1);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        Long cnt = countMap.remove(o);
        if (cnt != null && cnt != 1) {
            countMap.put((K) o, cnt - 1);
            return true;
        } else if (cnt == 1) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean containsAll(Collection<?> collection) {
        return countMap.keySet().containsAll(collection);
    }

    @Override
    public boolean addAll(Collection<? extends K> collection) {
        collection.forEach(this::add);
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> collection) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> collection) {
        boolean change = false;
        for (Object o : collection) {
            change |= remove(o);
        }
        return change;
    }

    @Override
    public void clear() {
        this.countMap = new HashMap<>();
    }
}
