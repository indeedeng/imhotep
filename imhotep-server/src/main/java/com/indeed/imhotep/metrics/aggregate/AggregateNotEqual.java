package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

import static com.indeed.imhotep.metrics.aggregate.AggregateStat.floaty;

/**
 * @author jwolfe
 */
public class AggregateNotEqual implements AggregateStat {
    private final AggregateStat lhs;
    private final AggregateStat rhs;

    public AggregateNotEqual(AggregateStat lhs, AggregateStat rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return floaty(lhs.apply(multiFTGSIterator) != rhs.apply(multiFTGSIterator));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateNotEqual that = (AggregateNotEqual) o;
        return Objects.equal(lhs, that.lhs) &&
                Objects.equal(rhs, that.rhs);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lhs, rhs);
    }

    @Override
    public String toString() {
        return "AggregateNotEqual{" +
                "lhs=" + lhs +
                ", rhs=" + rhs +
                '}';
    }
}
