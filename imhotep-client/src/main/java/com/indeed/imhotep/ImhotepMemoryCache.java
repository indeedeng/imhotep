/*
 * Copyright (C) 2014 Indeed Inc.
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

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author jsadun
 */
public class ImhotepMemoryCache<K, V extends MemoryMeasured> implements MemoryMeasured {
    private final Map<K, V> cache = new LinkedHashMap<K, V>();
    private long memoryUsed = 0;

    public synchronized @Nullable V tryRemove(K key) {
        final V val = cache.remove(key);
        if (val != null) {
            memoryUsed -= val.memoryUsed();
        }
        return val;
    }

    public synchronized void put(K key, V value)  {
        memoryUsed += value.memoryUsed();
        cache.put(key, value);
    }

    public synchronized @Nullable V poll() {
        final Iterator<V> iterator = cache.values().iterator();
        if (!iterator.hasNext()) {
            return null;
        }
        final V val = iterator.next();
        iterator.remove();
        memoryUsed -= val.memoryUsed();
        return val;
    }

    @Override
    public long memoryUsed() {
        return memoryUsed;
    }

    @Override
    public synchronized void close() {
        for (final V val : cache.values()) {
            val.close();
        }
        cache.clear();
        memoryUsed = 0;
    }

}
