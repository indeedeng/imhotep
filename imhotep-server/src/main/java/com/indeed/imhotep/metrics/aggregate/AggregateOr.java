package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

import static com.indeed.imhotep.metrics.aggregate.AggregateStat.floaty;
import static com.indeed.imhotep.metrics.aggregate.AggregateStat.truthy;

/**
 * @author jwolfe
 */
public class AggregateOr implements AggregateStat {
    private final AggregateStat lhs;
    private final AggregateStat rhs;

    public AggregateOr(AggregateStat lhs, AggregateStat rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return floaty(truthy(lhs.apply(multiFTGSIterator)) || truthy(rhs.apply(multiFTGSIterator)));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateOr that = (AggregateOr) o;
        return Objects.equal(lhs, that.lhs) &&
                Objects.equal(rhs, that.rhs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lhs, rhs);
    }

    @Override
    public String toString() {
        return "AggregateOr{" +
                "lhs=" + lhs +
                ", rhs=" + rhs +
                '}';
    }
}
