package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

public class AggregateRound implements AggregateStat{
    private final AggregateStat inner;

    public AggregateRound(AggregateStat inner) {
        this.inner = inner;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return Math.round(inner.apply(multiFTGSIterator));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateRound that = (AggregateRound) o;
        return Objects.equal(inner, that.inner);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inner);
    }

    @Override
    public String toString() {
        return "AggregateRound{" +
                "inner=" + inner +
                '}';
    }

}
