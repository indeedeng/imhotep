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

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.io.Shard;
import com.indeed.lsmtree.core.Store;

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class DatasetInfoList extends ObjectArrayList<DatasetInfo> {

    private static final Logger log = Logger.getLogger(DatasetInfoList.class);

    private static final Comparator<DatasetInfo> comparator =
        new Comparator<DatasetInfo>() {
        @Override public int compare(DatasetInfo thing1, DatasetInfo thing2) {
            return thing1.getDataset().compareTo(thing2.getDataset());
        }
    };

    DatasetInfoList(ShardMap shardMap) {
        for (Map.Entry<String, Object2ObjectOpenHashMap<String, Shard>>
                 datasetToShard : shardMap.entrySet()) {
            final String dataset = datasetToShard.getKey();
            final Element datasetInfo = new Element(dataset);
            for (Map.Entry<String, Shard>
                     idToShard : datasetToShard.getValue().entrySet()) {
                final String id    = idToShard.getKey();
                final Shard  shard = idToShard.getValue();
                try {
                    final ShardInfo shardInfo =
                        new ShardInfo(dataset, id, shard.getLoadedMetrics(),
                                      shard.getNumDocs(), shard.getShardVersion());
                    datasetInfo.add(shardInfo, shard);
                }
                catch (IOException ex) {
                    log.error("could not create Shard Info for dataset: "+ dataset +
                              " id: " + id, ex);
                }
            }
            datasetInfo.filter();
            add(datasetInfo);
        }
        Collections.sort(this, comparator);
    }

    DatasetInfoList(ShardStore store) {
        try {
            final Map<String, Element> datasetInfos =
                new Object2ObjectOpenHashMap<String, Element>();
            final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                store.iterator();

            while (it.hasNext()) {
                final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                final ShardStore.Key   key     = entry.getKey();
                final ShardStore.Value value   = entry.getValue();
                final String           dataset = key.getDataset();

                final ShardInfo shardInfo =
                    new ShardInfo(dataset, key.getShardId(),
                                  new ObjectArrayList<String>(0),
                                  value.getNumDocs(), value.getVersion());

                Element datasetInfo = datasetInfos.get(dataset);
                if (datasetInfo == null) {
                    datasetInfo = new Element(dataset);
                    datasetInfos.put(dataset, datasetInfo);
                }
                datasetInfo.add(shardInfo, value);
            }

            for (Element info: datasetInfos.values()) {
                info.filter();
            }
            addAll(datasetInfos.values());
        }
        catch (IOException ex) {
            /* TODO(johnf): consider allowing partial failure */
            log.error("failed to populate DatasetInfoList from ShardStore", ex);
        }
        Collections.sort(this, comparator);
    }

    private static final class Element extends DatasetInfo {

        private long newestVersion = -1;
        private ObjectOpenHashSet<String> newestIntFields = new ObjectOpenHashSet<String>();
        private ObjectOpenHashSet<String> newestStrFields = new ObjectOpenHashSet<String>();

        Element(String dataset) {
            super(dataset,
                  new ObjectArrayList<ShardInfo>(),
                  new ObjectOpenHashSet<String>(),
                  new ObjectOpenHashSet<String>(),
                  new ObjectOpenHashSet<String>());
        }

        void add(ShardInfo shardInfo, ShardStore.Value value) {
            getShardList().add(shardInfo);
            getIntFields().addAll(value.getIntFields());
            getStringFields().addAll(value.getStrFields());
            getMetrics().addAll(value.getIntFields());
            track(shardInfo.getVersion(), value.getIntFields(), value.getStrFields());
        }

        void add(ShardInfo shardInfo, Shard shard) throws IOException {
            getShardList().add(shardInfo);
            getIntFields().addAll(shard.getIntFields());
            getStringFields().addAll(shard.getStringFields());
            getMetrics().addAll(shard.getIntFields());
            track(shardInfo.getVersion(), shard.getIntFields(), shard.getStringFields());
        }

        private void track(long version,
                           Collection<String> intFields,
                           Collection<String> strFields) {
            if (version > newestVersion) {
                newestVersion = version;
                newestIntFields = new ObjectOpenHashSet<String>(intFields);
                newestStrFields = new ObjectOpenHashSet<String>(strFields);
            }
            else if (version == newestVersion) {
                newestIntFields.addAll(intFields);
                newestStrFields.addAll(strFields);
            }
        }

        private void filter() {
            getIntFields().removeAll(newestStrFields);
            getStringFields().removeAll(newestIntFields);

            final Set<String> conflicts =
                Sets.intersection(newestIntFields, newestStrFields);
            getIntFields().addAll(conflicts);
            getStringFields().addAll(conflicts);
        }
    }
}
