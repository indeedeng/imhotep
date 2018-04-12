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
            final Collection<String> stringFields) {
        super(shardId, numDocs, intFields, stringFields);
    }

    @Override
    public Set<String> getLoadedMetrics() {
        return Collections.emptySet();
    }
}
