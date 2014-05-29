package com.indeed.imhotep.client;

import com.google.common.collect.Maps;
import com.indeed.util.core.DataLoadingRunnable;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ImhotepRemoteSession;
import org.apache.log4j.Logger;

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
class ImhotepClientShardListReloader extends DataLoadingRunnable {
    private static final Logger log = Logger.getLogger(ImhotepClientShardListReloader.class);

    private final HostsReloader hostsReloader;
    private final ExecutorService rpcExecutor;

    private final Object shardListRpcLock = new Object();

    private volatile Map<Host, List<DatasetInfo>> shardList = Collections.emptyMap();

    ImhotepClientShardListReloader(HostsReloader hostsReloader, ExecutorService rpcExecutor) {
        super("ImhotepClientShardListReloader");
        
        this.hostsReloader = hostsReloader;
        this.rpcExecutor = rpcExecutor;
    }

    @Override
    public boolean load() {
        try {
            final Map<Host, List<DatasetInfo>> newShardList = shardListRpc();
            if (newShardList.isEmpty()) {
                log.error("unable to retrieve shard list from any imhotep daemons");
                loadFailed();
                return false;
            }
            if (!newShardList.equals(shardList)) {
                shardList = newShardList;
            }
            return true;
        } catch (Exception e) {
            log.error("Error reloading hosts", e);
            loadFailed();
            return false;
        }
    }

    public Map<Host, List<DatasetInfo>> getShardList() {
        return shardList;
    }

    private Map<Host, List<DatasetInfo>> shardListRpc() {
        synchronized (shardListRpcLock) {
            final List<Host> hosts = hostsReloader.getHosts();

            final Map<Host, Future<List<DatasetInfo>>> futures = Maps.newHashMap();
            for (final Host host : hosts) {
                final Future<List<DatasetInfo>> future = rpcExecutor.submit(new Callable<List<DatasetInfo>>() {
                    @Override
                    public List<DatasetInfo> call() throws Exception {
                        return ImhotepRemoteSession.getShardInfoList(host.hostname, host.port);
                    }
                });
                futures.put(host, future);
            }

            final Map<Host, List<DatasetInfo>> ret = Maps.newHashMapWithExpectedSize(hosts.size());
            for (final Host host : futures.keySet()) {
                try {
                    final List<DatasetInfo> shardList = futures.get(host).get();
                    ret.put(host, shardList);
                } catch (ExecutionException e) {
                    log.error("error getting shard list from " + host, e);
                } catch (InterruptedException e) {
                    log.error("error getting shard list from " + host, e);
                }
            }

            return ret;
        }
    }
}
