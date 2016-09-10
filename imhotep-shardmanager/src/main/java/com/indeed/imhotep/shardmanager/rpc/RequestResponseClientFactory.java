package com.indeed.imhotep.shardmanager.rpc;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ZkHostsReloader;
import com.indeed.imhotep.shardmanager.ShardManager;

import java.util.List;

/**
 * @author kenh
 */

public class RequestResponseClientFactory implements Supplier<ShardManager> {
    private final String zkNodes;
    private final String myHostname;

    public RequestResponseClientFactory(final String zkNodes, final String myHostname) {
        this.zkNodes = zkNodes;
        this.myHostname = myHostname;
    }

    @Override
    public RequestResponseClient get() {
        final List<Host> hosts = new ZkHostsReloader(zkNodes, "/imhotep/shardmanagers", true).getHosts();
        Preconditions.checkState(!hosts.isEmpty(), "Unable to get shardmanager endpoint");

        return new RequestResponseClient(hosts.get(Math.abs(myHostname.hashCode()) % hosts.size()));
    }
}
