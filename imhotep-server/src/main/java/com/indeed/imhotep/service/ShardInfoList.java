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

import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.io.Shard;
import com.indeed.lsmtree.core.Store;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/** ShardInfoList is an intermediate data structure used by
    LocalImhotepServiceCore to service GET_SHARD_LIST requests. Think of it as a
    specialized view of a ShardMap. */
class ShardInfoList extends ObjectArrayList<ShardInfo> {

    static final Comparator<ShardInfo> comparator = new Comparator<ShardInfo>() {
        @Override public int compare(final ShardInfo thing1, final ShardInfo thing2) {
            return thing1.shardId.compareTo(thing2.shardId);
        }
    };

    /** Build a list of ShardInfo objects from a ShardMap, sorted by shardId. */
    ShardInfoList(final ShardMap shardMap) throws IOException {
        shardMap.map(new ShardMap.ElementHandler<IOException>() {
                public void onElement(final String dataset,
                                      final String shardId,
                                      final Shard  shard) throws IOException {
                    final ShardInfo shardInfo =
                        new ShardInfo(shardId,
                                      shard.getNumDocs(),
                                      shard.getShardVersion());
                    add(shardInfo);
                }
            });
        Collections.sort(this, comparator);
    }

    /** Build a list of ShardInfo objects from a ShardStore, sorted by
        shardId. This method is intended for use by tests, not
        LocalImhotepServiceCore proper. */
    ShardInfoList(final ShardStore store) throws IOException {
        final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
            store.iterator();
        while (it.hasNext()) {
            final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
            final ShardStore.Key   key   = entry.getKey();
            final ShardStore.Value value = entry.getValue();
            final ShardInfo shardInfo =
                new ShardInfo(key.getShardId(),
                              value.getNumDocs(),
                              value.getVersion());
            add(shardInfo);
        }
        Collections.sort(this, comparator);
    }
}
