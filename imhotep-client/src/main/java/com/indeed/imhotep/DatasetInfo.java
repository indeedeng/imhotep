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

    public DatasetInfo(String dataset, Collection<ShardInfo> shardList, Collection<String> intFields, Collection<String> stringFields, Collection<String> metrics) {
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
