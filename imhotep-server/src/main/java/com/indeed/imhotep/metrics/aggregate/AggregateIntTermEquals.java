package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

import static com.indeed.imhotep.metrics.aggregate.AggregateStat.floaty;

/**
 * @author jwolfe
 */
public class AggregateIntTermEquals implements AggregateStat {
    private final long term;

    public AggregateIntTermEquals(long term) {
        this.term = term;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return floaty(term == multiFTGSIterator.termIntVal());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateIntTermEquals that = (AggregateIntTermEquals) o;
        return term == that.term;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(term);
    }

    @Override
    public String toString() {
        return "AggregateIntTermEquals{" +
                "term=" + term +
                '}';
    }
}
