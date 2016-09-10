package com.indeed.imhotep.client;

import com.indeed.util.core.DataLoadTimer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author kenh
 */

public class CheckpointedHostsReloader extends DataLoadTimer implements HostsReloader {
    private static final Logger LOGGER = Logger.getLogger(CheckpointedHostsReloader.class);

    private final File hostsFile;
    private final HostsReloader wrappedReloader;
    private final double dropThreshold;
    private volatile List<Host> hosts;

    public CheckpointedHostsReloader(final File hostsFile, final HostsReloader wrappedReloader, final double dropThreshold) throws IOException {
        this.hostsFile = hostsFile;
        this.wrappedReloader = wrappedReloader;
        this.dropThreshold = dropThreshold;
        if (hostsFile.exists()) {
            hosts = HostListSerializer.fromFile(LOGGER, hostsFile);
        } else {
            hosts = Collections.emptyList();
        }
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
            if (!temp.isEmpty() && (temp.size() >= (hosts.size() * dropThreshold))) {
                hosts = temp;
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
