package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

public class AggregateCeil implements AggregateStat{
    private final AggregateStat value;
    private final AggregateStat digits;

    public AggregateCeil(AggregateStat value, AggregateStat digits) {
        this.value = value;
        this.digits = digits;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        final double offset = Math.pow(10, digits.apply(multiFTGSIterator));
        return Math.ceil(value.apply(multiFTGSIterator) * offset) / offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateCeil that = (AggregateCeil) o;
        return Objects.equal(value, that.value) &&
                Objects.equal(digits, that.digits);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value, digits);
    }

    @Override
    public String toString() {
        return "AggregateCeil{" +
                "value=" + value +
                "digits=" + digits +
                '}';
    }

}
