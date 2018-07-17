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
import com.indeed.imhotep.protobuf.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author kenh
 */

class ShardMasterRequestHandler implements RequestHandler {
    private final ShardMaster shardMaster;
    private final int responseBatchSize;
    private static final Logger LOGGER = Logger.getLogger(ShardMasterRequestHandler.class);

    ShardMasterRequestHandler(final ShardMaster shardMaster, final int responseBatchSize) {
        this.shardMaster = shardMaster;
        this.responseBatchSize = responseBatchSize;
    }

    @Override
    public Iterable<ShardMasterResponse> handleRequest(final ShardMasterRequest request) throws IllegalArgumentException {
        switch (request.getRequestType()) {
            case GET_DATASET_METADATA:
                return handleDatasetMetadata();
            case GET_SHARD_LIST_FOR_TIME:
                return handleShardListForTime(request);
            case GET_SHARD_LIST:
                return handleShardList();
        }
        throw new IllegalArgumentException("request " + request +" does not have a valid type");
    }

    private Iterable<ShardMasterResponse> handleShardList() {
        try {
            final Map<String, List<ShardInfo>> shardList = shardMaster.getShardList();
            final ShardMasterResponse.Builder response = ShardMasterResponse.newBuilder();
            for(final String dataset: shardList.keySet()) {
                final DatasetMessage.Builder message = DatasetMessage.newBuilder().setDataset(dataset);

                for(final ShardInfo shard: shardList.get(dataset)) {
                    message.addShards(shard.toProto());
                }

                response.addAllShards(message);
            }
            return Collections.singletonList(response.setResponseCode(ShardMasterResponse.ResponseCode.OK).build());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get all shards", e);
        }
    }

    private Iterable<ShardMasterResponse> handleShardListForTime(ShardMasterRequest request) {
        try {
            final Iterable<Shard> shardsInTime = shardMaster.getShardsInTime(request.getDataset(), request.getStartTime(), request.getEndTime());
            final ShardMasterResponse.Builder builder = ShardMasterResponse.newBuilder();
            for(Shard shard: shardsInTime) {
                    ShardMessage.Builder message = ShardMessage.newBuilder()
                            .setDataset(request.getDataset())
                            .setHost(HostAndPort.newBuilder().setHost(shard.server.hostname).setPort(shard.server.port))
                            .setShardId(shard.shardId)
                            .setNumDocs(shard.numDocs)
                            .setVersion(shard.version)
                            .setExtension(shard.getExtension());
                    builder.addShardsInTime(message.build());
                }
            return Collections.singletonList(builder.setResponseCode(ShardMasterResponse.ResponseCode.OK).build());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get shards in time "+request.getStartTime()+"-"+request.getEndTime()+" for dataset "+request.getDataset(), e);
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
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get metadata", e);
        }
    }
}
