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

/**
 * @author kenh
 */

public interface ShardFilter {
    boolean accept(final String dataset);
    boolean accept(final String dataset, final String shardId);

    ShardFilter ACCEPT_ALL = new ShardFilter() {
        @Override
        public boolean accept(final String dataset) {
            return true;
        }

        @Override
        public boolean accept(final String dataset, final String shardId) {
            return true;
        }
    };
}
