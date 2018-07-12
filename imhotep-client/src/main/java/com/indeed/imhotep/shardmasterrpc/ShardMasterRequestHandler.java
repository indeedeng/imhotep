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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.ShardWithPathAndDataset;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.protobuf.AssignedShard;
import com.indeed.imhotep.protobuf.ShardMasterRequest;
import com.indeed.imhotep.protobuf.ShardMasterResponse;
import com.indeed.imhotep.protobuf.ShardMessage;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
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
            case GET_ASSIGNMENT:
                return handleAssignedShards(request);
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
            final List<ShardWithPathAndDataset> shardList = shardMaster.getShardList();
            final ShardMasterResponse.Builder builder = ShardMasterResponse.newBuilder();
            for(ShardWithPathAndDataset shard: shardList) {
                ShardMessage message = ShardMessage.newBuilder()
                        .setShardId(shard.shardId)
                        .setNumDocs(shard.numDocs)
                        .setVersion(shard.version)
                        .setPath(shard.getPath().toString())
                        .setDataset(shard.getDataset())
                        .addAllHosts(shard.servers.stream().map(Host::toString).collect(Collectors.toList()))
                        .build();
                builder.addAllShards(message);
            }
            return Collections.singletonList(builder.setResponseCode(ShardMasterResponse.ResponseCode.OK).build());
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
                            .addAllHosts(shard.getServers().stream().map(Host::toString).collect(Collectors.toList()))
                            .setShardId(shard.shardId)
                            .setNumDocs(shard.numDocs)
                            .setVersion(shard.version);
                    if (shard instanceof ShardWithPathAndDataset) {
                        message.setPath(((ShardWithPathAndDataset)shard).getPath().toString());
                    }
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

    private Iterable<ShardMasterResponse> handleAssignedShards(ShardMasterRequest request) {
        final Host node = new Host(request.getNode().getHost(), request.getNode().getPort());
        final Iterable<AssignedShard> assignedShards;
        try {
            assignedShards = shardMaster.getAssignments(node);
            System.out.println("assignedShards: "+assignedShards);
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to get shards for " + node, e);
        }

        return Iterables.transform(
                Iterables.partition(assignedShards, responseBatchSize),
                new Function<List<AssignedShard>, ShardMasterResponse>() {
                    @Override
                    public ShardMasterResponse apply(@Nullable final List<AssignedShard> shards) {
                        return ShardMasterResponse.newBuilder()
                                .setResponseCode(ShardMasterResponse.ResponseCode.OK)
                                .addAllAssignedShards(shards)
                                .build();
                    }
                });
    }
}
