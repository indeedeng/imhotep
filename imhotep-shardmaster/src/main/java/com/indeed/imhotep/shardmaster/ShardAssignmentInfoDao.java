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

package com.indeed.imhotep.shardmaster;

import com.indeed.imhotep.ShardDir;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.model.ShardAssignmentInfo;
import org.joda.time.DateTime;

import java.util.List;

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

    List<Host> getAssignments(String path);
}
