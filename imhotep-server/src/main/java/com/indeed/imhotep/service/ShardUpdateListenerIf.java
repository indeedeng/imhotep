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

package com.indeed.imhotep.service;

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;

import java.util.List;

public interface ShardUpdateListenerIf {

    /** Largely intended for testing purposes, Source allows listeners to
     * determine whether the provided shard info came from filesystem
     * examination or was loaded from a ShardStore cache. */
    enum Source { FILESYSTEM, CACHE }

    void onShardUpdate(final List<ShardInfo> shardList, final Source source);
    void onDatasetUpdate(final List<DatasetInfo> datasetList, final Source source);
}
