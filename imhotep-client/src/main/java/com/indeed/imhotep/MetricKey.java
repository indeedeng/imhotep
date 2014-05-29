package com.indeed.imhotep;

/**
 * @author jsadun
 */
public class MetricKey {
    private final String indexName;
    private final String shardName;
    private final String metricName;

    public MetricKey(String indexName, String shardName, String metricName) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MetricKey metricKey = (MetricKey) o;

        if (!indexName.equals(metricKey.indexName)) return false;
        if (!metricName.equals(metricKey.metricName)) return false;
        if (!shardName.equals(metricKey.shardName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = indexName.hashCode();
        result = 31 * result + shardName.hashCode();
        result = 31 * result + metricName.hashCode();
        return result;
    }
}
