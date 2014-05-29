package com.indeed.imhotep.client;

import com.indeed.imhotep.ShardInfo;

/**
 * @author jsgroth
 */
public interface ShardFilter {
    boolean accept(ShardInfo shard);
}
