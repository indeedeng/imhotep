package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

/**
 * @author jwolfe
 */
public class AggregateAbsoluteValue implements AggregateStat {
    private final AggregateStat inner;

    public AggregateAbsoluteValue(AggregateStat inner) {
        this.inner = inner;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return Math.abs(inner.apply(multiFTGSIterator));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateAbsoluteValue that = (AggregateAbsoluteValue) o;
        return Objects.equal(inner, that.inner);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inner);
    }

    @Override
    public String toString() {
        return "AggregateAbsoluteValue{" +
                "inner=" + inner +
                '}';
    }
}
