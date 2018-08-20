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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.indeed.util.core.DataLoadTimer;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Uses a static list of hosts in a consistent order but nulls out those that are down
 * based on the dynamic hosts reloader provided.
 */
public class StaticWithDynamicDowntimeHostsReloader extends DataLoadTimer implements HostsReloader {
    private final List<Host> allHosts;
    private final HostsReloader dynamicReloader;
    private volatile List<Host> hosts;

    public StaticWithDynamicDowntimeHostsReloader(List<Host> allHosts, HostsReloader dynamicReloader) throws IOException {
        this.allHosts = allHosts;
        this.dynamicReloader = dynamicReloader;
    }

    @Override
    public List<Host> getHosts() {
        return hosts;
    }

    @Override
    public void shutdown() {
        dynamicReloader.shutdown();
    }

    @Override
    public void run() {
        updateLastLoadCheck();

        dynamicReloader.run();

        if (!dynamicReloader.isLoadedDataSuccessfullyRecently()) {
            final Set<Host> upHosts = Sets.newHashSet(dynamicReloader.getHosts());
            final List<Host> newHostsWithDowntime = Lists.newArrayList(allHosts);
            for(int i = 0; i < newHostsWithDowntime.size(); i++) {
                if(!upHosts.contains(newHostsWithDowntime.get(i))) {
                    newHostsWithDowntime.set(i, null);  // this host is down
                }
            }
            hosts = newHostsWithDowntime;
            loadComplete();
            return;
        }
        loadFailed();
    }
}
