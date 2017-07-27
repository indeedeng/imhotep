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

import com.indeed.util.core.DataLoadingRunnable;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * @author jsgroth
 */
public class FileHostsReloader extends DataLoadingRunnable implements HostsReloader {
    private static final Logger log = Logger.getLogger(FileHostsReloader.class);

    private final String hostsFile;

    private volatile List<Host> hosts = Collections.emptyList();

    public FileHostsReloader(final String hostsFile) {
        super("FileHostsReloader");

        this.hostsFile = trimProtocol(hostsFile);
        updateHosts();
    }

    private static String trimProtocol(final String hostsFile) {
        if (hostsFile.startsWith("file://")) {
            return hostsFile.substring("file://".length());
        }
        return hostsFile;
    }

    @Override
    public List<Host> getHosts() {
        return hosts;
    }

    @Override
    public boolean load() {
        if (!updateHosts()) {
            loadFailed();
            return false;
        }
        return true;
    }

    @Override
    public void shutdown() {
    }

    private boolean updateHosts() {
        try {
            final List<Host> newHosts = HostListSerializer.fromFile(log, new File(hostsFile));
            log.info("reloaded hosts file, new list of hosts: "+newHosts);
            if (!newHosts.equals(hosts)) {
                hosts = newHosts;
            }
            return true;
        } catch (final Exception e) {
            log.error("error loading hosts file", e);
            return false;
        }
    }
}
