package com.indeed.imhotep.shardmaster;

/**
 * @author kenh
 */

interface ShardFilter {
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
