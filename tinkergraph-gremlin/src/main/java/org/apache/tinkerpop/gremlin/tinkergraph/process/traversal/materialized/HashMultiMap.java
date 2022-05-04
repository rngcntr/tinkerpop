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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class HashMultiMap<K,V> implements Map<K,V> {
    private HashMap<K,V> valueMap;
    private HashMap<K,Long> countMap;

    public HashMultiMap() {
        valueMap = new HashMap<>();
        countMap = new HashMap<>();
    }

    @Override
    public int size() {
        return valueMap.size();
    }

    @Override
    public boolean isEmpty() {
        return valueMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object o) {
        return valueMap.containsKey(o);
    }

    @Override
    public boolean containsValue(Object o) {
        return valueMap.containsValue(o);
    }

    @Override
    public V get(Object o) {
        return valueMap.get(o);
    }

    @Override
    public V put(K k, V v) {
        countMap.compute(k, (kk, vv) -> vv == null ? 1 : vv + 1);
        return valueMap.put(k, v);
    }

    @Override
    public V remove(Object o) {
        Long cnt = countMap.remove(o);
        if (cnt != null && cnt != 1) {
            countMap.put((K) o, cnt - 1);
            return valueMap.get(o);
        } else {
            return valueMap.remove(o);
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        map.forEach(this::put);
    }

    @Override
    public void clear() {
        valueMap.clear();
        countMap.clear();
    }

    @Override
    public Set<K> keySet() {
        return valueMap.keySet();
    }

    @Override
    public Collection<V> values() {
        return valueMap.values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return valueMap.entrySet();
    }
}
