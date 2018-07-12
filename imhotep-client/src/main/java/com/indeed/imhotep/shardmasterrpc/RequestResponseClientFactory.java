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

package com.indeed.imhotep.shardmasterrpc;

import com.google.common.base.Supplier;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.client.ZkHostsReloader;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author kenh
 */

public class RequestResponseClientFactory implements Supplier<ShardMaster> {
    private static final Logger LOGGER = Logger.getLogger(RequestResponseClientFactory.class);
    private final String zkNodes;
    private final String zkPath;
    private final Host myHost;

    public RequestResponseClientFactory(final String zkNodes, final String zkPath, final Host myHost) {
        this.zkNodes = zkNodes;
        this.zkPath = zkPath;
        this.myHost = myHost;
    }

    @Override
    public RequestResponseClient get() {
        final ZkHostsReloader zkHostsReloader = new ZkHostsReloader(zkNodes, zkPath, true);
        try {
            final List<Host> hosts = zkHostsReloader.getHosts();
            if (hosts.isEmpty()) {
                LOGGER.warn("Unable to get shardmaster endpoint under " + zkPath + " could not get client");
                return null;
            }
            return new RequestResponseClient(hosts.get(Math.abs(myHost.hashCode()) % hosts.size()));
        } finally {
            zkHostsReloader.shutdown();
        }
    }
}
