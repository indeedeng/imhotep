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

package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.imhotep.DynamicIndexSubshardDirnameUtil;
import com.indeed.imhotep.ImhotepMemoryCache;
import com.indeed.imhotep.ImhotepStatusDump.ShardDump;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.MetricKey;
import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.io.Shard;
import com.indeed.lsmtree.core.Store;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.ReloadableSharedReference;
import com.indeed.util.core.reference.SharedReference;
import it.unimi.dsi.fastutil.objects.Object2IntAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** ShardMap is the data structure used by LocalImhotepServiceCore to keep track
    of which shards reside on the host on which it is running. It's an unordered
    map of unordered maps, (dataset-&gt;(shardid-&gt;shard)).

    Conceptually, instances of this class are immutable, however nothing in
    practice prevents them from being modified, a property which is useful in
    the context of unit tests.

    WRT to its Map heritage, inheritence was chosen over composition in light
    of consumers such as DatasetInfoList, which need to iterate in specialized
    ways. I.e. wrapping a Map would provide better encapsulation, but
    necessitate more boilerplate wrapping code.
*/
class ShardMap
    extends Object2ObjectOpenHashMap<String, Object2ObjectOpenHashMap<String, Shard>> {

    private static final Logger log = Logger.getLogger(ShardMap.class);

    /** This class serves as a factory for FlamdexReaders, albeit in a
        roundabout fashion. In order to do so, it needs access to these
        resources (owned by LocalImhotepServiceCore). */
    private final MemoryReserver      memory;
    private final FlamdexReaderSource flamdexReaderSource;
    private final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache;

    private final ExecutorService threadPool = Executors.newCachedThreadPool();

    /** The general scheme for iterating over this collection is to pass an
        implementation of ElementHandler to the map() method. */
    interface ElementHandler<E extends Throwable> {
        void onElement(final String dataset,
                       final String shardId,
                       final Shard  shard) throws E;
    }

    /** Construct an empty ShardMap */
    ShardMap(final MemoryReserver      memory,
             final FlamdexReaderSource flamdexReaderSource,
             final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache) {
        this.memory              = memory;
        this.flamdexReaderSource = flamdexReaderSource;
        this.freeCache           = freeCache;
    }

    /** Construct a ShardMap containing the content of a ShardStore. I.e.,
        reconstitute it from its serialized form. */
    ShardMap(final ShardStore          store,
             final Path localShardsPath,
             final MemoryReserver      memory,
             final FlamdexReaderSource flamdexReaderSource,
             final ImhotepMemoryCache<MetricKey, IntValueLookup> freeCache)
        throws IOException {

        this(memory, flamdexReaderSource, freeCache);

        final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
            store.iterator();

        while (it.hasNext()) {
            final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
            final ShardStore.Key   key   = entry.getKey();
            final ShardStore.Value value = entry.getValue();
            final Path    datasetDir = localShardsPath.resolve(key.getDataset());
            final Path    indexDir   = datasetDir.resolve(value.getShardDir());
            final ShardId shardId = new ShardId(key.getDataset(),
                                                key.getShardId(),
                                                value.getVersion(),
                                                indexDir);

            final ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>
                loader = newLoader(indexDir, key.getDataset(), value.getShardDir());

            final Shard shard =
                new Shard(ReloadableSharedReference.create(loader),
                          shardId, value.getNumDocs(),
                          value.getIntFields(), value.getStrFields());
            putShard(key.getDataset(), shard);
        }
    }

    /** Construct a ShardMap by examining the shards stored on the local
        filesystem. This is the authoritative version of ShardMap. A reference
        ShardMap is required primarily so that we can internally reuse Shards
        which have already been loaded. */
    ShardMap(final ShardMap reference,
             final ShardDirIterator shardDirIterator)
        throws IOException {

        this(reference.memory, reference.flamdexReaderSource, reference.freeCache);

        for (final Pair<String, ShardDir> shard : shardDirIterator) {
            track(reference, shard.getFirst(), shard.getSecond());
        }
    }

    /** This is the preferred way to iterate over a ShardMap. ElementHandler's
        onElement() method will be invoked with each item in the collection. */
    <E extends Throwable> void map(final ElementHandler<E> handler) throws E {
        for (final Map.Entry<String, Object2ObjectOpenHashMap<String, Shard>>
                 datasetToShard : entrySet()) {
            final String dataset = datasetToShard.getKey();
            for (final Map.Entry<String, Shard>
                     idToShard : datasetToShard.getValue().entrySet()) {
                handler.onElement(dataset, idToShard.getKey(), idToShard.getValue());
            }
        }
    }

    /** This method synchronizes a ShardMap with its serialized form, a
        ShardStore. */
    void sync(final ShardStore store) throws IOException {
        if (store == null) {
            return;
        }
        saveTo(store);
        updateAndPrune(store);
        store.sync();
    }

    /** Produce a ShardDump containing the content of a ShardMap. */
    List<ShardDump> getShardDump() throws IOException {
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

    /** Produce a map of (dataset->number of shards). */
    Map<String, Integer> getShardCounts() {
        final Map<String, Integer> result = new Object2IntAVLTreeMap<>();
        for (final Map.Entry<String, Object2ObjectOpenHashMap<String, Shard>>
                 datasetToShard : entrySet()) {
            result.put(datasetToShard.getKey(), datasetToShard.getValue().size());
        }
        return result;
    }

    /** !@# This is a bit of a hack propagated from the old version of
        LocalImhotepServiceCore, which whilst opening sessions would note whether
        or not all shards referenced were Flamdexes. Why? Because that is a
        precondition to enabling native FTGS. */
    static final class FlamdexReaderMap
        extends Object2ObjectOpenHashMap<String, Pair<ShardId, CachedFlamdexReaderReference>> {
        public boolean allFlamdexReaders = true;
    }

    static final class CreateReader implements Callable<Boolean> {
        final String           request;
        final Shard            shard;
        final FlamdexReaderMap result;

        public CreateReader(final String request, final Shard shard, final FlamdexReaderMap result) {
            this.request = request;
            this.shard   = shard;
            this.result  = result;
        }

        public Boolean call() throws IOException, InterruptedException {
            final CachedFlamdexReaderReference reader;
            final ShardId shardId = shard.getShardId();
            final SharedReference<CachedFlamdexReader> reference = shard.getRef();
            boolean allFlamdexReaders = true;
            try {
                if (reference.get() instanceof RawCachedFlamdexReader) {
                    final SharedReference<RawCachedFlamdexReader> sharedReference =
                        (SharedReference<RawCachedFlamdexReader>) (SharedReference) reference;
                    reader = new RawCachedFlamdexReaderReference(sharedReference);
                }
                else {
                    allFlamdexReaders = false;
                    reader = new CachedFlamdexReaderReference(reference);
                }
            }
            catch (final NullPointerException ex) {
                log.warn("unable to create reader for shardId: " + shardId, ex);
                return Boolean.FALSE;
            }
            synchronized (result) {
                result.put(request, Pair.of(shardId, reader));
                result.allFlamdexReaders &= allFlamdexReaders;
            }
            return Boolean.TRUE;
        }
    }

    /** For each requested shard, return an id and a CachedFlamdexReaderReference. */
    FlamdexReaderMap getFlamdexReaders(final String dataset, final List<String> requestedShardIds)
        throws IOException {

        final Map<String, Shard> idToShard = get(dataset);
        if (idToShard == null) {
            throw new IllegalArgumentException("this service does not have dataset " + dataset);
        }

        final FlamdexReaderMap result = new FlamdexReaderMap();

        final List<CreateReader> createReaders = new ArrayList<>();

        for (final String request : requestedShardIds) {
            final Shard shard = idToShard.get(request);
            if (shard == null) {
                throw new IllegalArgumentException("this service does not have shard " +
                                                   request + " in dataset " + dataset);
            }
            createReaders.add(new CreateReader(request, shard, result));
        }

        /* Creating readers can actually be a bit expensive, since it can
           involve opening and reading metadata.txt files within shards. Do this
           in parallel, per IMTEPD-188. */
        try {
            final List<Future<Boolean>> outcomes =
                threadPool.invokeAll(createReaders, 5, TimeUnit.MINUTES);
            for (final Future<Boolean> outcome: outcomes) {
                if (outcome.get() == Boolean.FALSE) {
                    throw new IOException("unable to create all requested FlamdexReaders");
                }
            }
        }
        catch (final Throwable ex) {
            for (final Pair<ShardId, CachedFlamdexReaderReference> pair : result.values()) {
                final CachedFlamdexReaderReference cachedFlamdexReaderReference = pair.getSecond();
                Closeables2.closeQuietly(cachedFlamdexReaderReference, log);
            }
            throw new IOException("unable to create all requested FlamdexReaders " +
                                  "in a timely fashion", ex);
        }
        return result;
    }

    /** Return the Shard for a given (dataset, shardId) or null of the map
        doesn't contain it. */
    Shard getShard(final String dataset, final String shardId) {
        final Object2ObjectOpenHashMap<String, Shard> idToShard = get(dataset);
        return idToShard != null ? idToShard.get(shardId) : null;
    }

    /** Insert a Shard into the map for a given (dataset, shardId), replacing an
        existing one if present. */
    void putShard(final String dataset, final Shard shard) {
        Object2ObjectOpenHashMap<String, Shard> idToShard = get(dataset);
        if (idToShard == null) {
            idToShard = new Object2ObjectOpenHashMap<>();
            put(dataset, idToShard);
        }
        idToShard.put(shard.getShardId().getId(), shard);
    }

    private static boolean isNewerThan(@Nullable final ShardDir shardDir, @Nullable final Shard shard) {
        if (shardDir == null) {
            return false;
        }
        if (shard == null) {
            return true;
        }
        if (shardDir.getVersion() != shard.getShardVersion()) {
            return shardDir.getVersion() > shard.getShardVersion();
        }
        final Optional<DynamicIndexSubshardDirnameUtil.DynamicIndexShardInfo> firstInfo = DynamicIndexSubshardDirnameUtil.tryParse(shardDir.getName());
        final Optional<DynamicIndexSubshardDirnameUtil.DynamicIndexShardInfo> secondInfo = DynamicIndexSubshardDirnameUtil.tryParse(shard.getIndexDir().getFileName().toString());
        if (firstInfo.isPresent() && secondInfo.isPresent()) {
            return firstInfo.get().compareTo(secondInfo.get()) > 0;
        }
        return !shardDir.getIndexDir().equals(shard.getIndexDir());
    }

    private boolean track(final ShardMap reference, final String dataset, final ShardDir shardDir) {
        final Shard referenceShard = reference.getShard(dataset, shardDir.getId());
        final Shard currentShard   = getShard(dataset, shardDir.getId());

        if (isNewerThan(shardDir, referenceShard) && isNewerThan(shardDir, currentShard)) {
            final ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>
                loader = newLoader(shardDir.getIndexDir(), dataset, shardDir.getName());
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
            catch (final Exception ex) {
                log.warn("error loading shard at " + shardDir.getIndexDir(), ex);
                return false;
            }
        }
        else if ((currentShard != null) && currentShard.isNewerThan(referenceShard)) {
            putShard(dataset, currentShard);
        }
        else {
            putShard(dataset, referenceShard);
        }
        return false;
    }

    private void putShardToShardStore(final ShardStore shardStore, final ShardStore.Key key, final ShardDir shardDir, final Shard shard) throws IOException {
        final ShardStore.Value value = new ShardStore.Value(
                shardDir.getName(),
                shard.getNumDocs(),
                shard.getShardVersion(),
                new ObjectArrayList<>(shard.getIntFields()),
                new ObjectArrayList<>(shard.getStringFields())
        );
        shardStore.put(key, value);
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
                            putShardToShardStore(store, key, shardDir, shard);
                        }
                    }
                    catch (final IOException ex) {
                        log.error("failed to sync shard: " + key.toString(), ex);
                    }
                }
            });
    }

    private void updateAndPrune(final ShardStore store) {
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
                    catch (final IOException ex) {
                        log.warn("failed to updateAndPrune ShardStore item key: " +
                                 key.toString(), ex);
                    }
                } else {
                    if (!entry.getValue().getShardDir().equals(shard.getIndexDir().getFileName().toString())) {
                        final ShardDir shardDir = new ShardDir(shard.getIndexDir());
                        putShardToShardStore(store, key, shardDir, shard);
                    }
                }
            }
        }
        catch (final IOException ex) {
            log.warn("iteration over ShardStore failed during updateAndPrune operation", ex);
        }
    }

    private ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>
        newLoader(final Path   indexDir,
                  final String dataset,
                  final String shardDir) {
        return new ReloadableSharedReference.Loader<CachedFlamdexReader, IOException>() {
            @Override
                public CachedFlamdexReader load() throws IOException {
                final FlamdexReader flamdex = flamdexReaderSource.openReader(indexDir);
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
