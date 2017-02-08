package com.indeed.flamdex.datastruct;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class TestLong2ObjectSortedHashMap {

    @Test
    public void testSortedMapInterface() {
        final Long2ObjectSortedMap<Long> sortedMap = new Long2ObjectRBTreeMap<>();
        final Long2ObjectSortedHashMap<Long> sortedHashMap = new Long2ObjectSortedHashMap<>();
        final Random random = new Random(0);
        final List<Long> checkKeys = ImmutableList.of(Long.MIN_VALUE, -1L, 0L, 1L, 10L, 98L, 99L, 100L, Long.MAX_VALUE);

        for (int i = 0; i < 1000; ++i) {
            assertEquals(sortedMap, sortedHashMap);
            TestSortedHashMap.checkSortedMap(sortedHashMap);
            TestSortedHashMap.checkSubMaps(sortedHashMap, checkKeys);

            if (random.nextInt(3) == 0) {
                final long key = random.nextInt(100);
                assertEquals(sortedMap.remove(key), sortedHashMap.remove(key));
            } else if (random.nextInt(50) == 0) {
                sortedMap.clear();
                sortedHashMap.clear();
            } else {
                final long key = random.nextInt(100);
                final Long value = random.nextLong();
                assertEquals(sortedMap.put(key, value), sortedHashMap.put(key, value));
            }
        }
    }

    @Test
    public void testInternalState() {
        final Long2ObjectSortedMap<Long> sortedMap = new Long2ObjectRBTreeMap<>();
        final Long2ObjectMap<Long> hashMap = new Long2ObjectOpenHashMap<>();
        final Long2ObjectSortedHashMap<Long> sortedHashMap = new Long2ObjectSortedHashMap<>(sortedMap, hashMap);
        final Random random = new Random(0);

        for (int i = 0; i < 1000; ++i) {
            assertEquals(sortedHashMap, hashMap);
            assertEquals(sortedHashMap, sortedMap);
            if (random.nextInt(3) == 0) {
                final long key = random.nextInt(100);
                sortedHashMap.remove(key);
            } else if (random.nextInt(50) == 0) {
                sortedHashMap.clear();
            } else {
                final long key = random.nextInt(100);
                final Long value = random.nextLong();
                sortedHashMap.put(key, value);
            }
        }
    }
}