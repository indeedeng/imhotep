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

import com.google.common.collect.Sets;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import com.indeed.lsmtree.core.Store;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class DatasetInfoList extends ObjectArrayList<DatasetInfo> {

    private static final Logger log = Logger.getLogger(DatasetInfoList.class);

    static final Comparator<DatasetInfo> comparator = new Comparator<DatasetInfo>() {
        @Override public int compare(DatasetInfo thing1, DatasetInfo thing2) {
            return thing1.getDataset().compareTo(thing2.getDataset());
        }
    };

    private static final class LatestShardVersions
        extends Object2ObjectOpenHashMap<String, ShardStore.Value> {

        void track(ShardStore.Key key, ShardStore.Value current) {
            final String           dataset  = key.getDataset();
            final ShardStore.Value existing = get(dataset);
            if (existing == null || existing.getVersion() < current.getVersion()) {
                put(dataset, current);
            }
        }

        void filterFields(DatasetInfo info) {
            final ShardStore.Value value = get(info.getDataset());
            info.getIntFields().removeAll(value.getStrFields());
            info.getStringFields().removeAll(value.getIntFields());

            // for the fields that still conflict in the newest shard,
            // let the clients decide
            final Set<String> conflicts =
                Sets.intersection(new ObjectOpenHashSet<String>(value.getIntFields()),
                                  new ObjectOpenHashSet<String>(value.getStrFields()));
            info.getIntFields().addAll(conflicts);
            info.getStringFields().addAll(conflicts);
        }
    }

    DatasetInfoList(ShardStore store) {
        super(1024);            // TODO(johnf) figure out a better way to size this...
        try {
            final Map<String, DatasetInfo> datasetInfos =
                new Object2ObjectOpenHashMap<String, DatasetInfo>();
            final LatestShardVersions versions =
                new LatestShardVersions();
            final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it =
                store.iterator();

            while (it.hasNext()) {
                final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                final ShardStore.Key   key   = entry.getKey();
                final ShardStore.Value value = entry.getValue();
                versions.track(key, value);

                final ShardInfo shardInfo =
                    new ShardInfo(key.getDataset(), key.getShardId(),
                                  // !@# figure out how to populate this...
                                  new ObjectArrayList<String>(0),
                                  value.getNumDocs(),
                                  value.getVersion());
                final DatasetInfo datasetInfo = findOrCreate(datasetInfos, key.getDataset());
                datasetInfo.getShardList().add(shardInfo);
                datasetInfo.getIntFields().addAll(value.getIntFields());
                datasetInfo.getStringFields().addAll(value.getStrFields());
                datasetInfo.getMetrics().addAll(value.getIntFields());
            }

            for (DatasetInfo info: datasetInfos.values()) {
                versions.filterFields(info);
            }
            addAll(datasetInfos.values());
        }
        catch (IOException ex) {
            /* TODO(johnf): consider allowing partial failure */
            log.error("failed to populate DatasetInfoList from ShardStore", ex);
        }
        Collections.sort(this, comparator);
    }

    private static DatasetInfo findOrCreate(Map<String, DatasetInfo> datasetInfos,
                                            String dataset) {
        DatasetInfo result = datasetInfos.get(dataset);
        if (result == null) {
            result =
                new DatasetInfo(dataset,
                                new ObjectArrayList<ShardInfo>(),
                                new ObjectOpenHashSet<String>(),
                                new ObjectOpenHashSet<String>(),
                                new ObjectOpenHashSet<String>());
            datasetInfos.put(dataset, result);
        }
        return result;
    }
}
