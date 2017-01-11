package com.indeed.flamdex.datastruct;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectSortedMap;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSortedHashMap {

    private static <K, V> Comparator<? super K> getComparator(@Nonnull final SortedMap<K, V> sortedMap) {
        if (sortedMap.comparator() == null) {
            return new Comparator<K>() {
                @Override
                public int compare(final K o1, final K o2) {
                    @SuppressWarnings("unchecked")
                    final Comparable<K> comparableO1 = (Comparable<K>) o1;
                    return comparableO1.compareTo(o2);
                }
            };
        } else {
            return sortedMap.comparator();
        }
    }

    private static <T> void checkSorted(@Nonnull final Iterable<T> iterable, @Nonnull final Comparator<? super T> comparator) {
        final Iterator<T> iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            return;
        }
        T prev = iterator.next();
        while (iterator.hasNext()) {
            final T current = iterator.next();
            assertTrue(comparator.compare(prev, current) < 0);
            prev = current;
        }
    }

    static <K, V> void checkSortedMap(@Nonnull final SortedMap<K, V> sortedMap) {
        final int size = sortedMap.size();
        assertEquals(size == 0, sortedMap.isEmpty());
        assertEquals(size, Iterables.size(sortedMap.keySet()));
        assertEquals(size, Iterables.size(sortedMap.values()));
        assertEquals(size, Iterables.size(sortedMap.entrySet()));
        final Comparator<? super K> comparator = getComparator(sortedMap);
        checkSorted(sortedMap.keySet(), comparator);
        checkSorted(sortedMap.entrySet(), new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(final Map.Entry<K, V> o1, final Map.Entry<K, V> o2) {
                return comparator.compare(o1.getKey(), o2.getKey());
            }
        });
        final Iterable<V> sortedValues = FluentIterable.from(sortedMap.entrySet()).transform(new Function<Map.Entry<K, V>, V>() {
            @Override
            public V apply(final Map.Entry<K, V> entry) {
                return entry.getValue();
            }
        }).toList();
        for (final Map.Entry<K, V> entry : sortedMap.entrySet()) {
            assertEquals(entry.getValue(), sortedMap.get(entry.getKey()));
        }
        assertTrue(Iterables.elementsEqual(sortedValues, sortedMap.values()));
        if (!sortedMap.isEmpty()) {
            assertEquals(Iterables.getFirst(sortedMap.keySet(), null), sortedMap.firstKey());
            assertEquals(Iterables.getLast(sortedMap.keySet(), null), sortedMap.lastKey());
        }
    }

    static <K, V> void checkSubMaps(@Nonnull final SortedMap<K, V> actual, @Nonnull final Collection<K> keys) {
        final Comparator<? super K> comparator = getComparator(actual);
        final SortedMap<K, V> expected = new TreeMap<>(comparator);
        expected.putAll(actual);
        for (final K toKey : keys) {
            assertEquals(expected.headMap(toKey), actual.headMap(toKey));
            checkSortedMap(actual.headMap(toKey));
        }
        for (final K fromKey : keys) {
            assertEquals(expected.tailMap(fromKey), actual.tailMap(fromKey));
            checkSortedMap(actual.tailMap(fromKey));
        }
        for (final K fromKey : keys) {
            for (final K toKey : keys) {
                if (comparator.compare(fromKey, toKey) <= 0) {
                    assertEquals(expected.subMap(fromKey, toKey), actual.subMap(fromKey, toKey));
                    checkSortedMap(actual.subMap(fromKey, toKey));
                }
            }
        }
    }

    @Test
    public void testSortedMapInterface() {
        final Object2ObjectSortedMap<Long, Long> sortedMap = new Object2ObjectRBTreeMap<>();
        final SortedHashMap<Long, Long> sortedHashMap = new SortedHashMap<>();
        final Random random = new Random(0);
        final List<Long> checkKeys = ImmutableList.of(Long.MIN_VALUE, -1L, 0L, 1L, 10L, 98L, 99L, 100L, Long.MAX_VALUE);

        for (int i = 0; i < 1000; ++i) {
            assertEquals(sortedMap, sortedHashMap);
            checkSortedMap(sortedHashMap);
            checkSubMaps(sortedHashMap, checkKeys);

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
        final SortedHashMap<Long, Long> sortedHashMap = new SortedHashMap<>(sortedMap, hashMap);
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