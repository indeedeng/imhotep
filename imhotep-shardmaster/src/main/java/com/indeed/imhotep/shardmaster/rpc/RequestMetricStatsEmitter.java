package com.indeed.imhotep.shardmaster.rpc;

import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;

/**
 * interface to allow metrics reporting
 */
public interface RequestMetricStatsEmitter {
    void processed(String metricKey, ShardMasterRequest.RequestType requestType, long millis);

    RequestMetricStatsEmitter NULL_EMITTER = new RequestMetricStatsEmitter() {
        @Override
        public void processed(final String metricKey, final ShardMasterRequest.RequestType requestType, final long millis) {
        }
    };
}
