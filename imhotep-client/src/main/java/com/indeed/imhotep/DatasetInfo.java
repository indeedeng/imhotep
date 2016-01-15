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

/**
 * @author jsgroth
 */
public class DatasetInfo {
    private final String dataset;
    private final Collection<ShardInfo> shardList;
    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    private final Collection<String> metrics;

    public DatasetInfo(String                dataset,
                       Collection<ShardInfo> shardList,
                       Collection<String>    intFields,
                       Collection<String>    stringFields,
                       Collection<String>    metrics) {
        this.dataset = dataset;
        this.shardList = shardList;
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.metrics = metrics;
    }

    public String getDataset() {
        return dataset;
    }

    public Collection<ShardInfo> getShardList() {
        return shardList;
    }

    public Collection<String> getIntFields() {
        return intFields;
    }

    public Collection<String> getStringFields() {
        return stringFields;
    }

    public Collection<String> getMetrics() {
        return metrics;
    }

    public DatasetInfoMessage toProto() {
        return DatasetInfoMessage.newBuilder()
                .setDataset(dataset)
                .addAllShardInfo(Collections2.transform(shardList, new Function<ShardInfo, ShardInfoMessage>() {
                    @Override
                    public ShardInfoMessage apply(ShardInfo input) {
                        return input.toProto();
                    }
                }))
                .addAllIntField(intFields)
                .addAllStringField(stringFields)
                .addAllMetric(metrics)
                .build();
    }

    public static DatasetInfo fromProto(final DatasetInfoMessage proto) {
        return new DatasetInfo(
                proto.getDataset(),
                Lists.transform(proto.getShardInfoList(), new Function<ShardInfoMessage, ShardInfo>() {
                    @Override
                    public ShardInfo apply(ShardInfoMessage input) {
                        return ShardInfo.fromProto(input);
                    }
                }),
                proto.getIntFieldList(),
                proto.getStringFieldList(),
                proto.getMetricList()
        );
    }
}
