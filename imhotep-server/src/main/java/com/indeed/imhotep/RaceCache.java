/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep;

import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Cache that doesn't block. It may execute the loadFunction multiple times for the same key if several threads are
 * calling getOrLoad at the same time and the value is not cached.
 */
public class RaceCache<K,V,E extends Throwable> implements Closeable {
    private static final Logger log = Logger.getLogger(RaceCache.class);

    private final ConcurrentMap<K,V> map = new ConcurrentHashMap<>();
    private final LoadFunction<K, V, E> loadFunction;
    private final CloserFunction<V> closerFunction;


    public interface LoadFunction<K,V,E extends Throwable> {
        V load(K key) throws E;
    }

    public interface CloserFunction<V> {
        void close(V v);
    }

    public static <K,V extends Closeable,E extends Throwable> RaceCache<K,V,E> create(LoadFunction<K,V,E> loadFunction) {
        return new RaceCache<>(loadFunction, v -> Closeables2.closeQuietly(v, log));
    }

    public static <K,V,E extends Throwable> RaceCache<K,V,E> create(LoadFunction<K,V,E> loadFunction, CloserFunction<V> closerFunction) {
        return new RaceCache<>(loadFunction, closerFunction);
    }

    private RaceCache(LoadFunction<K,V,E> loadFunction, CloserFunction<V> closerFunction) {
        this.loadFunction = loadFunction;
        this.closerFunction = closerFunction;
    }

    public V getOrLoad(K key) throws E {
        final V v = map.get(key);
        if (v != null) {
            return v;
        }
        final V result = loadFunction.load(key);
        V old = map.putIfAbsent(key, result);
        if (old != null) {
            Closeables2.closeQuietly(() -> closerFunction.close(result), log);
            return old;
        }
        return result;
    }

    public void close() {
        Closeables2.closeAll(log, map.values().stream().<Closeable>map(v -> () -> closerFunction.close(v)).collect(Collectors.toList()));
    }
}
