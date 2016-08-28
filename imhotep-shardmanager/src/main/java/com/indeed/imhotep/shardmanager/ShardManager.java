package com.indeed.imhotep.shardmanager;

import com.indeed.imhotep.shardmanager.protobuf.AssignedShard;

import java.io.IOException;

/**
 * @author kenh
 */

public interface ShardManager {
    Iterable<AssignedShard> getAssignments(final String node) throws IOException;
}
