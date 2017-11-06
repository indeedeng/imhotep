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
 package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.indeed.imhotep.client.Host;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

public class Shard extends ShardInfo {
    public final List<Host> servers = Lists.newArrayList();


    public Shard(String shardId, int numDocs, long version) {
        super(shardId, numDocs, version);
    }

    public List<Host> getServers() {
        return servers;
    }


    /** Note that servers are not used in comparisons */
    @Override
    public int compareTo(@Nonnull ShardInfo o) {
        return super.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    public static List<String> keepShardIds(final List<Shard> shards) {
        final List<String> result = Lists.newArrayListWithCapacity(shards.size());
        for(final Shard shard : shards) {
            result.add(shard.getShardId());
        }
        return result;
    }
}
