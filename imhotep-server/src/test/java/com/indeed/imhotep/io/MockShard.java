package com.indeed.imhotep.io;

import com.indeed.imhotep.service.ShardId;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * @author vladimir
 */

public class MockShard extends Shard {
    public MockShard(ShardId shardId, int numDocs, Collection<String> intFields, Collection<String> stringFields, Collection<String> availableMetrics) {
        super(shardId, numDocs, intFields, stringFields, availableMetrics);
    }

    public Set<String> getLoadedMetrics() {
        return Collections.emptySet();
    }
}
