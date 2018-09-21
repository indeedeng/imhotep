package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

/**
 * @author jwolfe
 */
public class AggregateLog implements AggregateStat {
    private final AggregateStat inner;

    public AggregateLog(AggregateStat inner) {
        this.inner = inner;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return Math.log(inner.apply(multiFTGSIterator));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateLog that = (AggregateLog) o;
        return Objects.equal(inner, that.inner);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inner);
    }

    @Override
    public String toString() {
        return "AggregateLog{" +
                "inner=" + inner +
                '}';
    }
}
