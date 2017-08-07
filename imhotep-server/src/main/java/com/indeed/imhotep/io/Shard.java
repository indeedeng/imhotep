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
 package com.indeed.imhotep.io;

import com.google.common.annotations.VisibleForTesting;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.service.CachedFlamdexReader;
import com.indeed.imhotep.service.ShardId;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.ReloadableSharedReference;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;


public class Shard {
    private static final Logger log = Logger.getLogger(Shard.class);

    private final ReloadableSharedReference<CachedFlamdexReader, IOException> ref;
    private final ShardId shardId;
    private final int numDocs;
    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    private final Collection<String> availableMetrics;

    public Shard(final ReloadableSharedReference<CachedFlamdexReader, IOException> ref,
                  final long shardVersion,
                  final Path indexDir,
                  final String dataset,
                  final String shardId) throws IOException {
        this.ref = ref;
        this.shardId = new ShardId(dataset, shardId, shardVersion, indexDir);
        final SharedReference<CachedFlamdexReader> copy = ref.copy();
        numDocs = copy.get().getNumDocs();
        intFields = copy.get().getIntFields();
        stringFields = copy.get().getStringFields();
        availableMetrics = copy.get().getAvailableMetrics();
        copy.close();
    }

    public Shard(final ReloadableSharedReference<CachedFlamdexReader, IOException> ref,
                 final ShardId shardId,
                 final int numDocs,
                 final Collection<String> intFields,
                 final Collection<String> stringFields,
                 final Collection<String> availableMetrics) {
        this.ref = ref;
        this.shardId = shardId;
        this.numDocs = numDocs;
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.availableMetrics = availableMetrics;
    }

    @VisibleForTesting
    Shard(final ShardId shardId,
          final int numDocs,
          final Collection<String> intFields,
          final Collection<String> stringFields,
          final Collection<String> availableMetrics) {
        this(null, shardId, numDocs, intFields, stringFields, availableMetrics);
    }

    @Nullable
    public synchronized
    SharedReference<CachedFlamdexReader> getRef() throws IOException {
        return ref.copy();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public long getShardVersion() {
        return shardId.getShardVersion();
    }

    public Path getIndexDir() {
        return shardId.getIndexDir();
    }

    public String getDataset() {
        return shardId.getDataset();
    }

    public int getNumDocs() {
        return numDocs;
    }

    public Set<String> getLoadedMetrics() {
        final SharedReference<CachedFlamdexReader> copy = ref.copyIfLoaded();
        if (copy != null) {
            try {
                return copy.get().getLoadedMetrics();
            } finally {
                Closeables2.closeQuietly(copy, log);
            }
        }
        return Collections.emptySet();
    }

    public Collection<String> getIntFields() {
        return intFields;
    }

    public Collection<String> getStringFields() {
        return stringFields;
    }

    public Collection<String> getAvailableMetrics() {
        return availableMetrics;
    }

    public List<ImhotepStatusDump.MetricDump> getMetricDump() {
        final SharedReference<CachedFlamdexReader> copy = ref.copyIfLoaded();
        if (copy != null) {
            try {
                return copy.get().getMetricDump();
            } finally {
                Closeables2.closeQuietly(copy, log);
            }
        }
        return Collections.emptyList();
    }

    public boolean isNewerThan(final Shard otherShard) {
        if (otherShard == null) {
            return true;
        }
        return getShardVersion() > otherShard.getShardVersion();
    }
}
