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
 package com.indeed.imhotep;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
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

    public DatasetInfo(final String                dataset,
                       final Collection<ShardInfo> shardList,
                       final Collection<String>    intFields,
                       final Collection<String>    stringFields) {
        this.dataset = dataset;
        this.shardList = shardList;
        this.shardListRawMessages = null;
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    public DatasetInfo(final String                dataset,
                       final List<ShardInfoMessage> shardListRawMessages,
                       final Collection<String>    intFields,
                       final Collection<String>    stringFields) {
        this.dataset = dataset;
        this.shardListRawMessages = shardListRawMessages;
        this.shardList = null;
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    public String getDataset() {
        return dataset;
    }

    // this is deserialized on first access and then cached
    public synchronized Collection<ShardInfo> getShardList() {
        if(shardList == null) {
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

    public DatasetInfoMessage toProto() {
        return DatasetInfoMessage.newBuilder()
                .setDataset(dataset)
                .addAllShardInfo(Collections2.transform(shardList, new Function<ShardInfo, ShardInfoMessage>() {
                    @Override
                    public ShardInfoMessage apply(final ShardInfo input) {
                        return input.toProto();
                    }
                }))
                .addAllIntField(intFields)
                .addAllStringField(stringFields)
                .build();
    }

    public static DatasetInfo fromProto(final DatasetInfoMessage proto) {
        return new DatasetInfo(
                proto.getDataset(),
                proto.getShardInfoList(),
                proto.getIntFieldList(),
                proto.getStringFieldList()
        );
    }
}
