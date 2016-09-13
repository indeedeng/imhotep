package com.indeed.imhotep.client;

import com.indeed.util.core.DataLoadTimer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A hosts reloader that checkpoints the last successfully read value locally for resiliency.
 * This reloader also avoids large drops in the number of reloaded hosts.
 * This is to avoid transient large drops in reloaded hosts, that may result in overloading remaining hosts
 * @author kenh
 */

public class CheckpointedHostsReloader extends DataLoadTimer implements HostsReloader {
    private static final Logger LOGGER = Logger.getLogger(CheckpointedHostsReloader.class);

    private final File hostsFile;
    private final HostsReloader wrappedReloader;
    private final double dropThreshold;
    private volatile List<Host> hosts;
    private int maxHosts;

    public CheckpointedHostsReloader(final File hostsFile, final HostsReloader wrappedReloader, final double dropThreshold) throws IOException {
        this.hostsFile = hostsFile;
        this.wrappedReloader = wrappedReloader;
        this.dropThreshold = dropThreshold;
        if (hostsFile.exists()) {
            this.hosts = HostListSerializer.fromFile(LOGGER, hostsFile);
        } else {
            this.hosts = Collections.emptyList();
        }
        this.maxHosts = hosts.size();
    }

    @Override
    public List<Host> getHosts() {
        return hosts;
    }

    @Override
    public void shutdown() {
        wrappedReloader.shutdown();
    }

    @Override
    public void run() {
        updateLastLoadCheck();

        wrappedReloader.run();

        if (wrappedReloader.isLoadedDataSuccessfullyRecently()) {
            final List<Host> temp = wrappedReloader.getHosts();
            if (!temp.isEmpty() && (temp.size() >= (maxHosts * dropThreshold))) {
                hosts = temp;
                maxHosts = Math.max(hosts.size(), maxHosts);
                loadComplete();
                try {
                    HostListSerializer.toFile(LOGGER, temp, hostsFile);
                } catch (final IOException e) {
                    LOGGER.warn("Failed to persist hosts to " + hostsFile, e);
                }
                return;
            } else {
                LOGGER.warn("# of hosts dropped too much since last load, loaded hosts will be ignored");
            }
        }
        loadFailed();
    }
}
