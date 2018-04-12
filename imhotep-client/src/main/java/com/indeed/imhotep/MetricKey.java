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

/**
 * @author jsadun
 */
public class MetricKey {
    private final String indexName;
    private final String shardName;
    private final String metricName;

    public MetricKey(final String indexName, final String shardName, final String metricName) {
        this.indexName = indexName;
        this.shardName = shardName;
        this.metricName = metricName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getShardName() {
        return shardName;
    }

    public String getMetricName() {
        return metricName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final MetricKey metricKey = (MetricKey) o;

        return indexName.equals(metricKey.indexName)
                && metricName.equals(metricKey.metricName)
                && shardName.equals(metricKey.shardName);
    }

    @Override
    public int hashCode() {
        int result = indexName.hashCode();
        result = 31 * result + shardName.hashCode();
        result = 31 * result + metricName.hashCode();
        return result;
    }
}
