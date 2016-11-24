package com.indeed.imhotep.shardmaster;

import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.joda.time.DateTime;

/**
 * @author kenh
 */

public interface ShardAssignmentInfoDao {
    Iterable<ShardAssignmentInfo> getAssignments(final Host node);

    /**
     * updates the shard assignments for a given dataset.
     * This flushes out the old assignment, but with some time delay.
     * The reason for this is because during host fluctuation, there could be a transient state where a given
     * shard is allocated to no hosts, so to stay conservative, we retain old shard assignments for some time.
     */
    void updateAssignments(final String dataset, final DateTime timestamp, final Iterable<ShardAssignmentInfo> assignmentInfos);
}
