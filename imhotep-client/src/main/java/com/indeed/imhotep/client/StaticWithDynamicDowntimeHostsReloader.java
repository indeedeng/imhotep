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
import com.indeed.util.core.DataLoadingRunnable;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Uses a static list of hosts in a consistent order but nulls out those that are down
 * based on the dynamic hosts reloader provided.
 */
public class StaticWithDynamicDowntimeHostsReloader extends DataLoadingRunnable implements HostsReloader {
    private static final Logger log = Logger.getLogger(StaticWithDynamicDowntimeHostsReloader.class);
    private final List<Host> allHosts;
    private final HostsReloader dynamicReloader;
    private volatile List<Host> hosts = Collections.emptyList();

    public StaticWithDynamicDowntimeHostsReloader(List<Host> allHosts, HostsReloader dynamicReloader) {
        super("StaticWithDynamicDowntimeHostsReloader");
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
    public boolean load() {
        dynamicReloader.run();
        if (dynamicReloader.isLoadedDataSuccessfullyRecently()) {
            int downHosts = 0;
            final Set<Host> upHosts = Sets.newHashSet(dynamicReloader.getHosts());
            final List<Host> newHostsWithDowntime = Lists.newArrayList(allHosts);
            for(int i = 0; i < newHostsWithDowntime.size(); i++) {
                if(!upHosts.contains(newHostsWithDowntime.get(i))) {
                    newHostsWithDowntime.set(i, null);  // this host is down
                    downHosts++;
                }
            }
            hosts = newHostsWithDowntime;
            if(downHosts > 0) {
                log.warn(downHosts + " hosts are down out of " + allHosts.size() + ". " + upHosts.size() +
                        " returned by the dynamic hosts reloader");
            }
            return true;
        }
        log.warn("Dynamic hosts reloader is not up to date");
        return false;
    }
}
