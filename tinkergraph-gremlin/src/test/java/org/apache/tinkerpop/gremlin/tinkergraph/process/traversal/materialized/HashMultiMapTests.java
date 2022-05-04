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

import org.junit.Test;

import static org.junit.Assert.*;

public class HashMultiMapTests {

    @Test
    public void testPutSingle() {
        HashMultiMap<Integer, Boolean> map = new HashMultiMap<>();

        map.put(1, true);

        assertEquals(true, map.get(1));
        assertEquals(1, map.size());
        assertFalse(map.isEmpty());
    }

    @Test
    public void testPutTwice() {
        HashMultiMap<Integer, Boolean> map = new HashMultiMap<>();

        map.put(1, true);
        map.put(1, true);

        assertEquals(true, map.get(1));
        assertEquals(1, map.size());
        assertFalse(map.isEmpty());
    }

    @Test
    public void testPutMultiple() {
        HashMultiMap<Integer, Boolean> map = new HashMultiMap<>();

        map.put(1, true);
        map.put(2, false);

        assertEquals(true, map.get(1));
        assertEquals(false, map.get(2));
        assertEquals(2, map.size());
        assertFalse(map.isEmpty());
    }

    @Test
    public void testPutTwiceRemoveOnce() {
        HashMultiMap<Integer, Boolean> map = new HashMultiMap<>();

        map.put(1, true);
        map.put(1, true);
        map.remove(1);

        assertEquals(true, map.get(1));
        assertEquals(1, map.size());
        assertFalse(map.isEmpty());
    }

    @Test
    public void testPutTwiceRemoveTwice() {
        HashMultiMap<Integer, Boolean> map = new HashMultiMap<>();

        map.put(1, true);
        map.put(1, true);
        map.remove(1);
        map.remove(1);

        assertNull(map.get(1));
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }
}
