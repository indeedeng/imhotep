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

import it.unimi.dsi.fastutil.objects.Object2ObjectAVLTreeMap;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

class ShardCatalog implements AutoCloseable {
    private static final Logger log = Logger.getLogger(ShardCatalog.class);

    private final DatasetInfoStore diStore;
    private final ShardInfoStore   siStore;

    private final Map<String, DatasetInfo> diMap =
        new Object2ObjectAVLTreeMap<String, DatasetInfo>();

    private final Map<String, ShardInfo> siMap =
        new Object2ObjectAVLTreeMap<String, ShardInfo>();

    /* Note to self: we might need to pass in references to some of the gibby bits
     * in LocalImhotepServiceCore, namely 'memory', 'flamdexReaderFactory' and
     * 'freeCache'. (possibly others) ... only if we instantiate shards here though.
     */

    ShardCatalog(File disRoot, File sisRoot)
        throws IOException {

        diStore = new DatasetInfoStore(disRoot);
        try {
            siStore = new ShardInfoStore(sisRoot);
        }
        catch (IOException ex) {
            diStore.close();
            throw ex;
        }

        try {
            rehydrateDatasetInfo();
            rehydrateShardInfo();
        }
        catch (IOException ex) {
            close(diStore);
            close(siStore);
            throw ex;
        }
    }

    @Override public void close()
        throws IOException {
        close(diStore);
        close(siStore);
    }

    private void rehydrateDatasetInfo() throws IOException {
        Iterator<DatasetInfo> it = diStore.iterator();
        while (it.hasNext()) {
            final DatasetInfo di = it.next();
            diMap.put(di.getDataset(), di);
        }
    }

    private void rehydrateShardInfo() throws IOException {
        Iterator<ShardInfo> it = siStore.iterator();
        while (it.hasNext()) {
            final ShardInfo   si = it.next();
            final DatasetInfo di = diMap.get(si.getDataset());
            if (di != null) {
                siMap.put(si.getShardId(), si);
                di.getShardList().add(si);
            }
            else {
                log.warn("ShardInfo references an unknown dataset: " + si.getDataset() +
                         " -- ShardInfoStore and DatasetInfoStores must be out of sync");
            }
        }
    }

    private <V extends AutoCloseable> void close(V closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            }
            catch (Exception ex) {
                log.warn("failed to close resource: " +
                         closeable.getClass().getSimpleName() +
                         " -- its persistent form might be corrupt", ex);
            }
        }
    }
}
