package com.indeed.imhotep.client;

import com.indeed.imhotep.ShardInfo;

/**
 * @author jsgroth
 */
public class AcceptAllShardFilter implements ShardFilter {
    @Override
    public boolean accept(ShardInfo shard) {
        return true;
    }
}
