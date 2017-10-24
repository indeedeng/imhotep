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
 package com.indeed.imhotep.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.util.core.DataLoadingRunnable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author jsgroth
 */
class ImhotepClientMetadataReloader extends DataLoadingRunnable {
    private static final Logger log = Logger.getLogger(ImhotepClientMetadataReloader.class);

    private final HostsReloader hostsReloader;
    private final ExecutorService rpcExecutor;

    private final Object rpcLock = new Object();

    private volatile Map<String, DatasetInfo> datasetToDatasetInfo = Collections.emptyMap();

    ImhotepClientMetadataReloader(final HostsReloader hostsReloader, final ExecutorService rpcExecutor) {
        super("ImhotepClientMetadataReloader");
        
        this.hostsReloader = hostsReloader;
        this.rpcExecutor = rpcExecutor;
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
            final List<Host> hosts = hostsReloader.getHosts();

            final Map<Host, Future<List<DatasetInfo>>> futures = Maps.newHashMap();
            for (final Host host : hosts) {
                final Future<List<DatasetInfo>> future = rpcExecutor.submit(new Callable<List<DatasetInfo>>() {
                    @Override
                    public List<DatasetInfo> call() throws IOException {
                        return ImhotepRemoteSession.getDatasetMetadata(host.hostname, host.port);
                    }
                });
                futures.put(host, future);
            }

            final Collection<List<DatasetInfo>> perHostDatasets = Lists.newArrayListWithExpectedSize(hosts.size());
            for (final Map.Entry<Host, Future<List<DatasetInfo>>> hostFutureEntry : futures.entrySet()) {
                try {
                    final List<DatasetInfo> datasetInfos = hostFutureEntry.getValue().get();
                    perHostDatasets.add(datasetInfos);
                } catch (ExecutionException | InterruptedException e) {
                    log.error("error getting shard list from " + hostFutureEntry.getKey(), e);
                }
            }

            return ImhotepClient.getDatasetToDatasetInfo(perHostDatasets, false);
        }
    }
}
