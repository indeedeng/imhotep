package com.indeed.flamdex.datastruct;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectSortedMaps;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectCollection;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import it.unimi.dsi.fastutil.objects.ObjectSortedSets;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * HashMap that can iterate by sorted order, to be able to make get() faster
 *
 * @author michihiko
 */

public class Long2ObjectSortedMapWithHash<V> implements Long2ObjectSortedMap<V> {
    private final Long2ObjectSortedMap<V> sortedMap;
    private final Long2ObjectMap<V> hashMap;

    public Long2ObjectSortedMapWithHash() {
        this(new Long2ObjectRBTreeMap<V>(), new Long2ObjectOpenHashMap<V>());
    }

    public Long2ObjectSortedMapWithHash(final Long2ObjectSortedMap<V> sortedMap, final Long2ObjectMap<V> hashMap) {
        this.sortedMap = sortedMap;
        this.hashMap = hashMap;
    }

    public LongSet unorderedLongKeySet() {
        return hashMap.keySet();
    }

    public Collection<V> unorderedValues() {
        return hashMap.values();
    }

    public Set<Long2ObjectMap.Entry<V>> unorderedLong2ObjectEntrySet() {
        return Collections.unmodifiableSet(hashMap.long2ObjectEntrySet());
    }

    @Override
    public ObjectSortedSet<Map.Entry<Long, V>> entrySet() {
        return ObjectSortedSets.unmodifiable(sortedMap.entrySet());
    }

    @Override
    public ObjectSortedSet<Long2ObjectMap.Entry<V>> long2ObjectEntrySet() {
        return ObjectSortedSets.unmodifiable(sortedMap.long2ObjectEntrySet());
    }

    @Override
    public LongSortedSet keySet() {
        return sortedMap.keySet();
    }

    @Override
    public ObjectCollection<V> values() {
        return sortedMap.values();
    }

    @Override
    public LongComparator comparator() {
        return sortedMap.comparator();
    }

    @Override
    public Long2ObjectSortedMap<V> subMap(final Long fromKey, final Long toKey) {
        return Long2ObjectSortedMaps.unmodifiable(sortedMap.subMap(fromKey, toKey));
    }

    @Override
    public Long2ObjectSortedMap<V> headMap(final Long toKey) {
        return Long2ObjectSortedMaps.unmodifiable(sortedMap.headMap(toKey));
    }

    @Override
    public Long2ObjectSortedMap<V> tailMap(final Long fromKey) {
        return Long2ObjectSortedMaps.unmodifiable(sortedMap.tailMap(fromKey));
    }

    @Override
    public Long firstKey() {
        return sortedMap.firstKey();
    }

    @Override
    public Long lastKey() {
        return sortedMap.lastKey();
    }

    @Override
    public Long2ObjectSortedMap<V> subMap(final long fromKey, final long toKey) {
        return Long2ObjectSortedMaps.unmodifiable(sortedMap.subMap(fromKey, toKey));
    }

    @Override
    public Long2ObjectSortedMap<V> headMap(final long toKey) {
        return Long2ObjectSortedMaps.unmodifiable(sortedMap.headMap(toKey));
    }

    @Override
    public Long2ObjectSortedMap<V> tailMap(final long fromKey) {
        return Long2ObjectSortedMaps.unmodifiable(sortedMap.tailMap(fromKey));
    }

    @Override
    public long firstLongKey() {
        return sortedMap.firstLongKey();
    }

    @Override
    public long lastLongKey() {
        return sortedMap.lastLongKey();
    }

    @Override
    public V put(final long l, final V v) {
        sortedMap.put(l, v);
        return hashMap.put(l, v);
    }

    @Override
    public V get(final long l) {
        return hashMap.get(l);
    }

    @Override
    public V remove(final long l) {
        sortedMap.remove(l);
        return hashMap.remove(l);
    }

    @Override
    public boolean containsKey(final long l) {
        return hashMap.containsKey(l);
    }

    @Override
    public void defaultReturnValue(final V v) {
        sortedMap.defaultReturnValue(v);
        hashMap.defaultReturnValue(v);
    }

    @Override
    public V defaultReturnValue() {
        return hashMap.defaultReturnValue();
    }

    @Override
    public V put(final Long aLong, final V v) {
        sortedMap.put(aLong, v);
        return hashMap.put(aLong, v);
    }

    @Override
    public V get(final Object o) {
        return hashMap.get(o);
    }

    @Override
    public boolean containsKey(final Object o) {
        return hashMap.containsKey(o);
    }

    @Override
    public boolean containsValue(final Object value) {
        return hashMap.containsValue(value);
    }

    @Override
    public V remove(final Object o) {
        sortedMap.remove(o);
        return hashMap.remove(o);
    }

    @Override
    public void putAll(final Map<? extends Long, ? extends V> m) {
        sortedMap.putAll(m);
        hashMap.putAll(m);
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
    public void clear() {
        sortedMap.clear();
        hashMap.clear();
    }
}
