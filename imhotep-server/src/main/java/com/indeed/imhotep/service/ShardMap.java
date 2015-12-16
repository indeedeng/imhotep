/*
 * Copyright (C) 2015 Indeed Inc.
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

package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.imhotep.ImhotepMemoryCache;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.MetricKey;
import com.indeed.imhotep.io.Shard;
import com.indeed.lsmtree.core.Store;
import com.indeed.util.core.reference.ReloadableSharedReference;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/* (dataset->(shardid->shard))

   TODO: figure out how to properly synchronize this
*/
class ShardMap
    extends Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, Shard>> {

    private static final Logger log = Logger.getLogger(ShardMap.class);

    private static final class OnlyDirs implements FilenameFilter {
        public boolean accept(File dir, String name) {
            final File file = new File(dir, name);
            return file.exists() && file.isDirectory();
        }
    }

    final MemoryReserver      memory;
    final FlamdexReaderSource flamdexReaderSource;
    final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache;

    ShardMap(final ShardStore          store,
             final File                localShardsPath,
             final MemoryReserver      memory,
             final FlamdexReaderSource flamdexReaderSource,
             final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache)
        throws IOException {

        this.memory              = memory;
        this.flamdexReaderSource = flamdexReaderSource;
        this.freeCache           = freeCache;

        final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
            store.iterator();

        while (it.hasNext()) {
            final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
            final ShardStore.Key   key   = entry.getKey();
            final ShardStore.Value value = entry.getValue();
            final File    datasetDir = new File(localShardsPath, key.getDataset());
            final File    indexDir   = new File(datasetDir, value.getShardDir());
            final ShardId shardId    = new ShardId(key.getDataset(), key.getShardId(),
                                                   value.getVersion(),
                                                   indexDir.getCanonicalPath());

            final ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>
                loader = newLoader(indexDir, key.getDataset(), value.getShardDir());

            final Shard shard =
                new Shard(ReloadableSharedReference.create(loader),
                          shardId, value.getNumDocs(),
                          value.getIntFields(), value.getStrFields(),
                          value.getIntFields());
            getShardMap(key.getDataset()).put(key.getShardId(), shard);
        }
    }

    ShardMap(ShardMap reference, String shardsPath)
        throws IOException {
        boolean dirty = false;
        final OnlyDirs onlyDirs = new OnlyDirs();
        for (final File datasetDir : new File(shardsPath).listFiles(onlyDirs)) {
            final String dataset = datasetDir.getName();
            for (final File file : datasetDir.listFiles(onlyDirs)) {
                final ShardDir shardDir = new ShardDir(file);
                if (shardMap.track(reference, dataset, shardDir)) {
                    dirty = true;
                }
            }
        }
        if (prune(store)) {
            dirty = true;
        }
    }

    private Map<String, Shard> getShardMap(String dataset) {
        Object2ObjectOpenHashMap<String, Shard> result = get(dataset);
        if (result == null) {
            result = new Object2ObjectOpenHashMap<String, Shard>();
            put(dataset, result);
        }
        return result;
    }

    // !@# blech - this shouldn't create a dataset entry as a side-effect
    private Shard getShard(String dataset, String shardId) {
        return getShardMap(dataset).get(shardId);
    }

    private Shard putShard(String dataset, Shard shard) {
        return getShardMap(dataset).put(shard.getShardId().getId(), shard);
    }

    private boolean track(ShardStore store, ShardMap reference, String dataset, ShardDir shardDir) {
        final Shard knownShard = reference.getShard(dataset, shardDir.getId());
        if (shardDir.isNewerThan(knownShard)) {
            final ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>
                loader = newLoader(new File(shardDir.getIndexDir()), dataset, shardDir.getName());
            try {
                final Shard shard =
                    new Shard(ReloadableSharedReference.create(loader),
                              shardDir.getVersion(),
                              shardDir.getIndexDir(),
                              dataset,
                              shardDir.getId());
                final ShardStore.Key key = new ShardStore.Key(dataset, shardDir.getId());
                final ShardStore.Value value =
                    new ShardStore.Value(shardDir.getName(),
                                         shard.getNumDocs(),
                                         shard.getShardVersion(),
                                         new ObjectArrayList(shard.getIntFields()),
                                         new ObjectArrayList(shard.getStringFields()));
                store.put(key, value);
                putShard(dataset, shard);
                // !@# log a "loaded shard ..." message
                return true;
            }
            catch (IOException exception) {
                // !@# log an error...
                return false;
            }
        }
        else {
            putShard(dataset, knownShard);
        }
        return false;
    }

    /** @return true if we pruned something */
    private boolean prune(ShardStore store) {
        final List<ShardStore.Key> keysToDelete = new ObjectArrayList<ShardStore.Key>();

        final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
            store.iterator();

        while (it.hasNext()) {
            final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
            final ShardStore.Key key = entry.getKey();
            final Shard shard = getShard(key.getDataset(), key.getShardId());
            if (shard == null) {
                keysToDelete.add(key);
            }
        }

        // !@# TODO(johnf): do we need to delete after the fact or are
        // !LSMTree iterators valid in the face of deletions?
        for (ShardStore key : keysToDelete) {
            store.delete(key);
        }
        return keysToDelete.size() > 0;
    }

    private ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>
        newLoader(final File   indexDir,
                  final String dataset,
                  final String shardDir) {
        return new ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>() {
            @Override
                public CachedFlamdexReader load() throws IOException {
                final FlamdexReader flamdex =
                    flamdexReaderSource.openReader(indexDir.getCanonicalPath());
                if (flamdex instanceof RawFlamdexReader) {
                    return new RawCachedFlamdexReader(new MemoryReservationContext(memory),
                                                      (RawFlamdexReader) flamdex,
                                                      dataset, shardDir, freeCache);
                }
                else {
                    return new CachedFlamdexReader(new MemoryReservationContext(memory),
                                                   flamdex, dataset, shardDir, freeCache);
                }
            }
        };
    }
}
