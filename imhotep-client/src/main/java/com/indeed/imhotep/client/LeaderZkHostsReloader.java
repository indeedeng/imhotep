package com.indeed.imhotep.client;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author kornerup
 */

public class LeaderZkHostsReloader extends ZkHostsReloader {
    private static final Logger log = Logger.getLogger(LeaderZkHostsReloader.class);

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
            Collections.sort(children);
            this.hosts = children.parallelStream().map(childName -> zkPath + "/" + childName).map(child -> {
                try {
                    return new String(zkConnection.getData(child, false, new Stat())).split(":");
                } catch (KeeperException | InterruptedException | IndexOutOfBoundsException e) {
                    log.error("could not get data for child: " + child);
                    return null;
                }
            }).filter(Objects::nonNull).map(data -> new Host(data[0], Integer.parseInt(data[1]))).collect(Collectors.toList());
        } catch (KeeperException | InterruptedException e) {
            loadFailed();
            return false;
        }
        loadComplete();
        return true;
    }
}
