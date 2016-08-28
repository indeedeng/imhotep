package com.indeed.imhotep.shardmanager;

import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmanager.model.ShardAssignmentInfo;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

/**
 * @author kenh
 */

@ThreadSafe
interface ShardAssigner {
    Iterable<ShardAssignmentInfo> assign(List<Host> hosts, final String dataset, final List<ShardDir> shards);
}
