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
 package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.indeed.imhotep.protobuf.DatasetInfoMessage;
import com.indeed.imhotep.protobuf.ShardInfoMessage;

import java.util.Collection;
import java.util.List;

/**
 * @author jsgroth
 */
public class DatasetInfo {
    private final String dataset;
    private Collection<ShardInfo> shardList;
    private List<ShardInfoMessage> shardListRawMessages;
    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    protected long latestShardVersion;

    public DatasetInfo(final String dataset,
                       final Collection<ShardInfo> shardList,
                       final Collection<String> intFields,
                       final Collection<String> stringFields,
                       final long latestShardVersion) {
        this.dataset = dataset;
        this.shardList = shardList;
        this.latestShardVersion = latestShardVersion;
        this.shardListRawMessages = null;
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    public DatasetInfo(final String dataset,
                       final List<ShardInfoMessage> shardListRawMessages,
                       final Collection<String> intFields,
                       final Collection<String> stringFields,
                       final long latestShardVersion) {
        this.dataset = dataset;
        this.shardListRawMessages = shardListRawMessages;
        this.latestShardVersion = latestShardVersion;
        this.shardList = null;
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    public String getDataset() {
        return dataset;
    }

    // this is deserialized on first access and then cached
    public synchronized Collection<ShardInfo> getShardList() {
        if(shardList == null && shardListRawMessages != null) {
            List<ShardInfo> deserializedShardList = Lists.newArrayListWithCapacity(shardListRawMessages.size());
            for(ShardInfoMessage shardInfoMessage : shardListRawMessages) {
                deserializedShardList.add(ShardInfo.fromProto(shardInfoMessage));
            }
            shardList = deserializedShardList;
            shardListRawMessages = null;
        }
        return shardList;
    }

    public Collection<String> getIntFields() {
        return intFields;
    }

    public Collection<String> getStringFields() {
        return stringFields;
    }

    public long getLatestShardVersion() {
        return latestShardVersion;
    }

    public void setLatestShardVersion(long latestShardVersion) {
        this.latestShardVersion = latestShardVersion;
    }

    public DatasetInfoMessage toProto() {
        final List<ShardInfoMessage> serializedShardList = Lists.newArrayListWithCapacity(shardList.size());
        for(ShardInfo shardInfo: shardList) {
            serializedShardList.add(shardInfo.toProto());
        }

        return DatasetInfoMessage.newBuilder()
                .setDataset(dataset)
                .addAllShardInfo(serializedShardList)
                .addAllIntField(intFields)
                .addAllStringField(stringFields)
                .setLatestShardVersion(latestShardVersion)
                .build();
    }

    public DatasetInfoMessage toProtoNoShards() {
        return DatasetInfoMessage.newBuilder()
                .setDataset(dataset)
                .addAllIntField(intFields)
                .addAllStringField(stringFields)
                .setLatestShardVersion(latestShardVersion)
                .build();
    }

    public static DatasetInfo fromProto(final DatasetInfoMessage proto) {
        return new DatasetInfo(
                proto.getDataset(),
                proto.getShardInfoList(),
                proto.getIntFieldList(),
                proto.getStringFieldList(),
                proto.getLatestShardVersion());
    }
}
