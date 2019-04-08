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

package com.indeed.imhotep.shardmasterrpc;

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.protobuf.DatasetShardsMessage;
import com.indeed.imhotep.protobuf.ShardMasterRequest;
import com.indeed.imhotep.protobuf.ShardMasterResponse;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * @author kenh
 */

class ShardMasterRequestHandler implements RequestHandler {
    private final ShardMaster shardMaster;
    private final int responseBatchSize;

    ShardMasterRequestHandler(final ShardMaster shardMaster, final int responseBatchSize) {
        this.shardMaster = shardMaster;
        this.responseBatchSize = responseBatchSize;
    }

    @Override
    public Iterable<ShardMasterResponse> handleRequest(final ShardMasterRequest request) {
        switch (request.getRequestType()) {
            case GET_DATASET_METADATA:
                return handleDatasetMetadata();
            case GET_SHARD_LIST_FOR_TIME:
                return handleShardListForTime(request);
            case GET_SHARD_LIST:
                return handleShardList();
            case REFRESH_FIELDS_FOR_DATASET:
                return handleRefreshFieldsForDataet(request);
        }
        throw new IllegalArgumentException("request " + request + " does not have a valid type");
    }

    private Iterable<ShardMasterResponse> handleRefreshFieldsForDataet(final ShardMasterRequest request) {
        try {
            shardMaster.refreshFieldsForDataset(request.getDatasetToRefresh());
            return Collections.singletonList(ShardMasterResponse.newBuilder().setResponseCode(ShardMasterResponse.ResponseCode.OK).build());
        } catch (final IOException e) {
            return Collections.singletonList(ShardMasterResponse.newBuilder().setResponseCode(ShardMasterResponse.ResponseCode.ERROR).build());
        }
    }

    private Iterable<ShardMasterResponse> handleShardList() {
        try {
            final Map<String, Collection<ShardInfo>> shardList = shardMaster.getShardList();
            final ShardMasterResponse.Builder response = ShardMasterResponse.newBuilder();
            for (final String dataset : shardList.keySet()) {
                final DatasetShardsMessage.Builder message = DatasetShardsMessage.newBuilder().setDataset(dataset);

                for (final ShardInfo shard : shardList.get(dataset)) {
                    message.addShards(shard.toProto());
                }

                response.addAllShards(message);
            }
            return Collections.singletonList(response.setResponseCode(ShardMasterResponse.ResponseCode.OK).build());
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get all shards", e);
        }
    }

    private Iterable<ShardMasterResponse> handleShardListForTime(final ShardMasterRequest request) {
        try {
            final Iterable<Shard> shardsInTime = shardMaster.getShardsInTime(request.getDataset(), request.getStartTime(), request.getEndTime());
            final ShardMasterResponse.Builder builder = ShardMasterResponse.newBuilder();
            for (final Shard shard : shardsInTime) {
                shard.addToShardMessage(builder.addShardsInTimeBuilder().setDataset(request.getDataset()));
            }
            return Collections.singletonList(builder.setResponseCode(ShardMasterResponse.ResponseCode.OK).build());
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shards for time range " + request.getStartTime() + "-" + request.getEndTime() + " for dataset " + request.getDataset(), e);
        }
    }

    private Iterable<ShardMasterResponse> handleDatasetMetadata() {
        try {
            final Iterable<DatasetInfo> datasetMetadata = shardMaster.getDatasetMetadata();
            final ShardMasterResponse.Builder collect = StreamSupport.stream(datasetMetadata.spliterator(), false)
                    .map(DatasetInfo::toProto).collect(
                            ShardMasterResponse::newBuilder,
                            ShardMasterResponse.Builder::addMetadata,
                            (a, b) -> a.addAllMetadata(b.getMetadataList()));
            return Collections.singleton(collect.setResponseCode(ShardMasterResponse.ResponseCode.OK).build());
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get metadata", e);
        }
    }
}
