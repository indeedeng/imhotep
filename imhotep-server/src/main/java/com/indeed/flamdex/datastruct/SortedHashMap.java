package com.indeed.flamdex.datastruct;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * HashMap that can iterate by sorted order, to be able to make get() faster
 *
 * @author michihiko
 */

public class SortedHashMap<K, V> implements SortedMap<K, V> {
    private final SortedMap<K, V> sortedMap;
    private final Map<K, V> hashMap;

    public SortedHashMap() {
        this(new TreeMap<K, V>(), new HashMap<K, V>());
    }

    public SortedHashMap(final SortedMap<K, V> sortedMap, final Map<K, V> hashMap) {
        this.sortedMap = sortedMap;
        this.hashMap = hashMap;
    }

    @Override
    public Comparator<? super K> comparator() {
        return sortedMap.comparator();
    }

    @Nonnull
    @Override
    public SortedMap<K, V> subMap(final K fromKey, final K toKey) {
        return Collections.unmodifiableSortedMap(sortedMap.subMap(fromKey, toKey));
    }

    @Nonnull
    @Override
    public SortedMap<K, V> headMap(final K toKey) {
        return Collections.unmodifiableSortedMap(sortedMap.headMap(toKey));
    }

    @Nonnull
    @Override
    public SortedMap<K, V> tailMap(final K fromKey) {
        return Collections.unmodifiableSortedMap(sortedMap.tailMap(fromKey));
    }

    @Override
    public K firstKey() {
        return sortedMap.firstKey();
    }

    @Override
    public K lastKey() {
        return sortedMap.lastKey();
    }

    @Override
    public int size() {
        return sortedMap.size();
    }

    @Override
    public boolean isEmpty() {
        return sortedMap.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return hashMap.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return hashMap.containsValue(value);
    }

    @Override
    public V get(final Object key) {
        return hashMap.get(key);
    }

    @Override
    public V put(final K key, final V value) {
        sortedMap.put(key, value);
        return hashMap.put(key, value);
    }

    @Override
    public V remove(final Object key) {
        sortedMap.remove(key);
        return hashMap.remove(key);
    }

    @Override
    public void putAll(@Nonnull final Map<? extends K, ? extends V> m) {
        sortedMap.putAll(m);
        hashMap.putAll(m);
    }

    @Override
    public void clear() {
        sortedMap.clear();
        hashMap.clear();
    }

    @Nonnull
    @Override
    public Set<K> keySet() {
        return Collections.unmodifiableSet(sortedMap.keySet());
    }

    @Nonnull
    @Override
    public Collection<V> values() {
        return Collections.unmodifiableCollection(sortedMap.values());
    }

    @Nonnull
    @Override
    public Set<Entry<K, V>> entrySet() {
        return Collections.unmodifiableSet(sortedMap.entrySet());
    }

    public Set<K> unorderedKeySet() {
        return Collections.unmodifiableSet(hashMap.keySet());
    }

    public Collection<V> unorderedValues() {
        return Collections.unmodifiableCollection(hashMap.values());
    }

    public Set<Entry<K, V>> unorderedEntrySet() {
        return Collections.unmodifiableSet(hashMap.entrySet());
    }

    @Override
    public int hashCode() {
        return sortedMap.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        return hashMap.equals(o);
    }
}
