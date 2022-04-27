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
