package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

import static com.indeed.imhotep.metrics.aggregate.AggregateStat.floaty;
import static com.indeed.imhotep.metrics.aggregate.AggregateStat.truthy;

/**
 * @author jwolfe
 */
public class AggregateNot implements AggregateStat {
    private final AggregateStat inner;

    public AggregateNot(AggregateStat inner) {
        this.inner = inner;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return floaty(!truthy(inner.apply(multiFTGSIterator)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateNot that = (AggregateNot) o;
        return Objects.equal(inner, that.inner);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inner);
    }

    @Override
    public String toString() {
        return "AggregateNot{" +
                "inner=" + inner +
                '}';
    }
}
