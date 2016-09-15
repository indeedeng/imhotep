package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.base.Supplier;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ZkHostsReloader;
import com.indeed.imhotep.shardmaster.ShardMaster;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author kenh
 */

public class RequestResponseClientFactory implements Supplier<ShardMaster> {
    private static final Logger LOGGER = Logger.getLogger(RequestResponseClientFactory.class);
    private final String zkNodes;
    private final String zkPath;
    private final String myHostname;

    public RequestResponseClientFactory(final String zkNodes, final String zkPath, final String myHostname) {
        this.zkNodes = zkNodes;
        this.zkPath = zkPath;
        this.myHostname = myHostname;
    }

    @Override
    public RequestResponseClient get() {
        final List<Host> hosts = new ZkHostsReloader(zkNodes, zkPath, true).getHosts();

        if (hosts.isEmpty()) {
            LOGGER.warn("Unable to get shardmaster endpoint under " + zkPath + " could not get client");
            return null;
        }

        return new RequestResponseClient(hosts.get(Math.abs(myHostname.hashCode()) % hosts.size()));
    }
}
