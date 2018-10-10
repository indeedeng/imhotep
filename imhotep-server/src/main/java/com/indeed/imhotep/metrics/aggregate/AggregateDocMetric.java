package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

import java.util.Arrays;

/**
 * @author jwolfe
 */
public class AggregateDocMetric implements AggregateStat {
    private final int sessionIndex;
    private final int statIndex;

    public AggregateDocMetric(int sessionIndex, int statIndex) {
        this.sessionIndex = sessionIndex;
        this.statIndex = statIndex;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return (double) multiFTGSIterator.groupStat(sessionIndex, statIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateDocMetric that = (AggregateDocMetric) o;
        return sessionIndex == that.sessionIndex &&
                statIndex == that.statIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(sessionIndex, statIndex);
    }

    @Override
    public String toString() {
        return "AggregateDocMetric{" +
                "sessionIndex=" + sessionIndex +
                ", statIndex=" + statIndex +
                '}';
    }
}
