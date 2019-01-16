package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

public class AggregateCeil implements AggregateStat{
    private final AggregateStat inner;

    public AggregateCeil(AggregateStat inner) {
        this.inner = inner;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return Math.ceil(inner.apply(multiFTGSIterator));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateCeil that = (AggregateCeil) o;
        return Objects.equal(inner, that.inner);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inner);
    }

    @Override
    public String toString() {
        return "AggregateCeil{" +
                "inner=" + inner +
                '}';
    }

}
