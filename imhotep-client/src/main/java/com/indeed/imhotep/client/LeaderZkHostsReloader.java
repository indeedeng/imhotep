package com.indeed.imhotep.client;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author kornerup
 */

public class LeaderZkHostsReloader extends ZkHostsReloader {
    public LeaderZkHostsReloader(final String zkNodes, final boolean readHostsBeforeReturning) {
        super(zkNodes, readHostsBeforeReturning);
    }

    public LeaderZkHostsReloader(final String zkNodes, final String zkPath, final boolean readHostsBeforeReturning) {
        super(zkNodes, zkPath, readHostsBeforeReturning);
        if (readHostsBeforeReturning) {
            load();
        }
    }

    @Override
    public boolean load() {
        try {
            final List<String> children = zkConnection.getChildren(zkPath, false);
            final String[] foundHost = new String(zkConnection.getData(zkPath+"/"+children.get(0), false, new Stat())).split(":");
            final String hostname = foundHost[0];
            final int port = Integer.parseInt(foundHost[1]);
            Host host = new Host(hostname, port);
            this.hosts = Collections.singletonList(host);
        } catch (KeeperException | InterruptedException | IndexOutOfBoundsException e) {
            return false;
        }
        return true;
    }
}
