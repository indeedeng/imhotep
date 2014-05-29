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
