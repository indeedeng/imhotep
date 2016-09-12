package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ZkHostsReloader;
import com.indeed.imhotep.shardmaster.ShardMaster;

import java.util.List;

/**
 * @author kenh
 */

public class RequestResponseClientFactory implements Supplier<ShardMaster> {
    private final String zkNodes;
    private final String myHostname;

    public RequestResponseClientFactory(final String zkNodes, final String myHostname) {
        this.zkNodes = zkNodes;
        this.myHostname = myHostname;
    }

    @Override
    public RequestResponseClient get() {
        final List<Host> hosts = new ZkHostsReloader(zkNodes, "/imhotep/shardmasters", true).getHosts();
        Preconditions.checkState(!hosts.isEmpty(), "Unable to get shardmaster endpoint");

        return new RequestResponseClient(hosts.get(Math.abs(myHostname.hashCode()) % hosts.size()));
    }
}
