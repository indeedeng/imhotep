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
import com.indeed.lsmtree.core.Store;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/** TODO(johnf): figure out synchronization needs
 */
class ShardInfoList extends ObjectArrayList<ShardInfo> {
    private static final Logger log = Logger.getLogger(ShardInfoList.class);

    static final Comparator<ShardInfo> comparator = new Comparator<ShardInfo>() {
        @Override public int compare(ShardInfo thing1, ShardInfo thing2) {
            final int result = thing1.dataset.compareTo(thing2.dataset);
            return result != 0 ? result : thing1.shardId.compareTo(thing2.shardId);
        }
    };

    ShardInfoList(ShardStore store) {
        super(4096);            // TODO(johnf) figure out a better way to size this...
        try {
            final Iterator<Store.Entry<ShardStore.Key, ShardStore.Value>> it = store.iterator();
            while (it.hasNext()) {
                final Store.Entry<ShardStore.Key, ShardStore.Value> entry = it.next();
                final ShardStore.Key   key   = entry.getKey();
                final ShardStore.Value value = entry.getValue();
                final ShardInfo shardInfo =
                    new ShardInfo(key.getDataset(), key.getShardId(),
                                  // !@# figure out how to populate this...
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
