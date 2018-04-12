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

import com.indeed.util.core.DataLoadTimer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A hosts reloader that checkpoints the last successfully read value locally for resiliency.
 * This reloader also avoids large drops in the number of reloaded hosts.
 * It remembers the max number of hosts it has seen, and if it drops below the configured threshold,
 * the reloader will refuse to refresh the hosts list.
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
                LOGGER.warn("loaded hosts of " + temp.size() + " dropped too much since max hosts " + maxHosts + ", loaded hosts will be ignored");
            }
        }
        loadFailed();
    }
}
