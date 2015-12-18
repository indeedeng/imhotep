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
import com.indeed.imhotep.ImhotepStatusDump.ShardDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.MetricKey;
import com.indeed.imhotep.io.Shard;
import com.indeed.lsmtree.core.Store;
import com.indeed.util.core.Pair;
import com.indeed.util.core.reference.ReloadableSharedReference;
import com.indeed.util.core.reference.SharedReference;

import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/* (dataset->(shardid->shard)) */
class ShardMap
    extends Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, Shard>> {

    private static final Logger log = Logger.getLogger(ShardMap.class);

    final MemoryReserver      memory;
    final FlamdexReaderSource flamdexReaderSource;
    final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache;

    interface ElementHandler<E extends Throwable> {
        void onElement(final String dataset,
                       final String shardId,
                       final Shard  shard) throws E;
    }

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
            putShard(key.getDataset(), shard);
        }
    }

    ShardMap(final ShardMap   reference,
             final String     shardsPath,
             final MemoryReserver memory,
             final FlamdexReaderSource flamdexReaderSource,
             final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache)
        throws IOException {

        this.memory              = memory;
        this.flamdexReaderSource = flamdexReaderSource;
        this.freeCache           = freeCache;

        final OnlyDirs onlyDirs = new OnlyDirs();
        for (final File datasetDir : new File(shardsPath).listFiles(onlyDirs)) {
            final String dataset = datasetDir.getName();
            for (final File file : datasetDir.listFiles(onlyDirs)) {
                final ShardDir shardDir = new ShardDir(file);
                track(reference, dataset, shardDir);
            }
        }
    }

    public <E extends Throwable> void map(ElementHandler<E> handler) throws E {
        for (Map.Entry<String, Object2ObjectOpenHashMap<String, Shard>>
                 datasetToShard : entrySet()) {
            final String dataset = datasetToShard.getKey();
            for (Map.Entry<String, Shard>
                     idToShard : datasetToShard.getValue().entrySet()) {
                handler.onElement(dataset, idToShard.getKey(), idToShard.getValue());
            }
        }
    }

    public void sync(ShardStore store) {
        saveTo(store);
        prune(store);
    }

    public List<ShardDump> getShardDump() throws IOException {
        final List<ShardDump> result = new ObjectArrayList<>();
        map(new ElementHandler<IOException>() {
                public void onElement(final String dataset,
                                      final String shardId,
                                      final Shard  shard) throws IOException {
                    result.add(new ShardDump(shardId, dataset,
                                             shard.getNumDocs(),
                                             shard.getMetricDump()));
                }
            });
        return result;
    }

    public Map<String, Integer> getShardCounts() {
        final Map<String, Integer> result = new Object2IntAVLTreeMap<>();
        for (Map.Entry<String, Object2ObjectOpenHashMap<String, Shard>>
                 datasetToShard : entrySet()) {
            result.put(datasetToShard.getKey(), datasetToShard.getValue().size());
        }
        return result;
    }

    /* !@# This is a bit of a hack propagated from the old version of
       LocalImhotepServiceCore, which whilst opening sessions would note whether
       or not all shards referenced were Flamdexes. Why? Because that is a
       precondition to enabling native FTGS. */
    static final class FlamdexReaderMap
        extends Object2ObjectOpenHashMap<String, Pair<ShardId, CachedFlamdexReaderReference>> {
        public boolean allFlamdexReaders = true;
    }

    public FlamdexReaderMap getFlamdexReaders(String dataset, List<String> requestedShardIds)
        throws IOException {

        final Map<String, Shard> idToShard = get(dataset);
        if (idToShard == null) {
            throw new IllegalArgumentException("this service does not have dataset " + dataset);
        }

        final FlamdexReaderMap result = new FlamdexReaderMap();

        for (String request : requestedShardIds) {
            final Shard shard = idToShard.get(request);
            if (shard == null) {
                throw new IllegalArgumentException("this service does not have shard " +
                                                   request + " in dataset " + dataset);
            }
            final CachedFlamdexReaderReference reader;
            final ShardId shardId = shard.getShardId();
            final SharedReference<CachedFlamdexReader> reference = shard.getRef();
            if (reference.get() instanceof RawCachedFlamdexReader) {
                final SharedReference<RawCachedFlamdexReader> sharedReference =
                    (SharedReference<RawCachedFlamdexReader>) (SharedReference) reference;
                reader = new RawCachedFlamdexReaderReference(sharedReference);
                result.allFlamdexReaders = false;
            }
            else {
                reader = new CachedFlamdexReaderReference(reference);
            }
            result.put(request, Pair.of(shardId, reader));
        }
        return result;
    }

    private Shard getShard(String dataset, String shardId) {
        final Object2ObjectOpenHashMap<String, Shard> idToShard = get(dataset);
        return idToShard != null ? idToShard.get(shardId) : null;
    }

    private void putShard(String dataset, Shard shard) {
        Object2ObjectOpenHashMap<String, Shard> idToShard = get(dataset);
        if (idToShard == null) {
            idToShard = new Object2ObjectOpenHashMap<>();
            put(dataset, idToShard);
        }
        idToShard.put(shard.getShardId().getId(), shard);
    }

    private boolean track(ShardMap reference, String dataset, ShardDir shardDir) {
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
                putShard(dataset, shard);
                log.debug("loading shard " + shardDir.getId() +
                          " from " + shardDir.getIndexDir());
                return true;
            }
            catch (IOException ex) {
                log.warn("error loading shard at " + shardDir.getIndexDir(), ex);
                return false;
            }
        }
        else {
            putShard(dataset, knownShard);
        }
        return false;
    }

    private void saveTo(final ShardStore store) {
        map(new ElementHandler<RuntimeException>() {
                public void onElement(final String dataset,
                                      final String shardId,
                                      final Shard  shard) {
                    final ShardStore.Key key = new ShardStore.Key(dataset, shardId);
                    try {
                        if (!store.containsKey(key)) {
                            final ShardDir shardDir = new ShardDir(shard.getIndexDir());
                            final ShardStore.Value value =
                                new ShardStore.Value(shardDir.getName(),
                                                     shard.getNumDocs(),
                                                     shard.getShardVersion(),
                                                     new ObjectArrayList(shard.getIntFields()),
                                                     new ObjectArrayList(shard.getStringFields()));
                            store.put(key, value);
                        }
                    }
                    catch (IOException ex) {
                        log.error("failed to sync shard: " + key.toString(), ex);
                    }
                }
            });
    }

    private void prune(ShardStore store) {
        try {
            final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                store.iterator();

            while (it.hasNext()) {
                final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                final ShardStore.Key key = entry.getKey();
                final Shard shard = getShard(key.getDataset(), key.getShardId());
                if (shard == null) {
                    try {
                        store.delete(key);
                    }
                    catch (IOException ex) {
                        log.warn("failed to prune ShardStore item key: " +
                                 key.toString(), ex);
                    }
                }
            }
        }
        catch (IOException ex) {
            log.warn("iteration over ShardStore failed during prune operation", ex);
        }
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

    private static final class OnlyDirs implements FilenameFilter {
        public boolean accept(File dir, String name) {
            final File file = new File(dir, name);
            return file.exists() && file.isDirectory();
        }
    }
}
