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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

class ShardInfoList extends ObjectArrayList<ShardInfo> {

    private static final Logger log = Logger.getLogger(ShardInfoList.class);

    static final Comparator<ShardInfo> comparator = new Comparator<ShardInfo>() {
        @Override public int compare(ShardInfo thing1, ShardInfo thing2) {
            final int result = thing1.dataset.compareTo(thing2.dataset);
            return result != 0 ? result : thing1.shardId.compareTo(thing2.shardId);
        }
    };

    ShardInfoList(ShardMap shardMap) {
        for (Map.Entry<String, Object2ObjectOpenHashMap<String, Shard>>
                 datasetToShard : shardMap.entrySet()) {
            final String dataset = datasetToShard.getKey();
            for (Map.Entry<String, Shard>
                     idToShard : datasetToShard.getValue().entrySet()) {
                final String id    = idToShard.getKey();
                final Shard  shard = idToShard.getValue();
                try {
                    final ShardInfo shardInfo =
                        new ShardInfo(dataset, id, shard.getLoadedMetrics(),
                                      shard.getNumDocs(), shard.getShardVersion());
                    add(shardInfo);
                }
                catch (IOException ex) {
                    log.error("could not create Shard Info for dataset: "+ dataset +
                              " id: " + id, ex);
                }
            }
        }
    }

    ShardInfoList(ShardStore store) {
        try {
            final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                store.iterator();
            while (it.hasNext()) {
                final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                final ShardStore.Key   key   = entry.getKey();
                final ShardStore.Value value = entry.getValue();
                final ShardInfo shardInfo =
                    new ShardInfo(key.getDataset(), key.getShardId(),
                                  new ObjectArrayList<String>(0),
                                  value.getNumDocs(),
                                  value.getVersion());
                add(shardInfo);
            }
        }
        catch (IOException ex) {
            /* TODO(johnf): consider allowing partial failure */
            log.error("failed to populate ShardInfoList from ShardStore", ex);
        }
        Collections.sort(this, comparator);
    }
}
