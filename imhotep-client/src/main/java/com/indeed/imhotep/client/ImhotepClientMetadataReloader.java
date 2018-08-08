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
package com.indeed.imhotep.client;

import com.google.common.base.Throwables;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.shardmasterrpc.ShardMaster;
import com.indeed.util.core.DataLoadingRunnable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

/**
 * @author jsgroth
 */
class ImhotepClientMetadataReloader extends DataLoadingRunnable {
    private static final Logger log = Logger.getLogger(ImhotepClientMetadataReloader.class);

    private final Object rpcLock = new Object();
    private final Supplier<ShardMaster> shardMasterSupplier;

    private volatile Map<String, DatasetInfo> datasetToDatasetInfo = Collections.emptyMap();

    ImhotepClientMetadataReloader(Supplier<ShardMaster> shardMasterSupplier) {
        super("ImhotepClientMetadataReloader");
        this.shardMasterSupplier = shardMasterSupplier;
    }

    @Override
    public boolean load() {
        try {
            final Map<String, DatasetInfo> newDatasetToDatasetInfo = getDatasetToDatasetInfoRpc();
            if (newDatasetToDatasetInfo.isEmpty()) {
                log.error("unable to retrieve dataset info from any imhotep daemons");
                loadFailed();
                return false;
            }
            datasetToDatasetInfo = newDatasetToDatasetInfo;
            return true;
        } catch (final Exception e) {
            log.error("Error reloading hosts", e);
            loadFailed();
            return false;
        }
    }

    public Map<String, DatasetInfo> getDatasetToDatasetInfo() {
        return datasetToDatasetInfo;
    }

    private Map<String, DatasetInfo> getDatasetToDatasetInfoRpc() {
        synchronized (rpcLock) {

                Map<String, DatasetInfo> toReturn = new HashMap<>();
            final List<DatasetInfo> datasetInfos;
            try {
                datasetInfos = shardMasterSupplier.get().getDatasetMetadata();
                datasetInfos.forEach(datasetInfo -> toReturn.put(datasetInfo.getDataset(), datasetInfo));
                return toReturn;
            } catch (IOException e) {
                log.error("Failed to get dataset metadata", e);
                throw Throwables.propagate(e);
            }
        }
    }
}
