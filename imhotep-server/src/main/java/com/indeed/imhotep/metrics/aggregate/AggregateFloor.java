package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

public class AggregateFloor implements AggregateStat{
    private final AggregateStat inner;

    public AggregateFloor(AggregateStat inner) {
        this.inner = inner;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return Math.floor(inner.apply(multiFTGSIterator));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateFloor that = (AggregateFloor) o;
        return Objects.equal(inner, that.inner);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inner);
    }

    @Override
    public String toString() {
        return "AggregateFloor{" +
                "inner=" + inner +
                '}';
    }

}
