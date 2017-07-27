package com.indeed.imhotep.io;

import com.indeed.imhotep.service.ShardId;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * @author vladimir
 */

public class MockShard extends Shard {
    public MockShard(
            final ShardId shardId,
            final int numDocs,
            final Collection<String> intFields,
            final Collection<String> stringFields,
            final Collection<String> availableMetrics) {
        super(shardId, numDocs, intFields, stringFields, availableMetrics);
    }

    @Override
    public Set<String> getLoadedMetrics() {
        return Collections.emptySet();
    }
}
