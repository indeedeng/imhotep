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

/**
* @author jsgroth
 *
 * additional config parameters for LocalImhotepServiceCore with provided defaults
*/
public final class LocalImhotepServiceConfig {
    private int updateShardsFrequencySeconds = 120;
    private int heartBeatCheckFrequencySeconds = 60;

    public int getUpdateShardsFrequencySeconds() {
        return updateShardsFrequencySeconds;
    }

    public int getHeartBeatCheckFrequencySeconds() {
        return heartBeatCheckFrequencySeconds;
    }

    public LocalImhotepServiceConfig setUpdateShardsFrequencySeconds(int updateShardsFrequencySeconds) {
        this.updateShardsFrequencySeconds = updateShardsFrequencySeconds;
        return this;
    }

    public LocalImhotepServiceConfig setHeartBeatCheckFrequencySeconds(int heartBeatCheckFrequencySeconds) {
        this.heartBeatCheckFrequencySeconds = heartBeatCheckFrequencySeconds;
        return this;
    }
}
