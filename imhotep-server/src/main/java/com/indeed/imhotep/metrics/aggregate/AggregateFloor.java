package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

public class AggregateFloor implements AggregateStat{
    private final AggregateStat value;
    private final AggregateStat digits;

    public AggregateFloor(AggregateStat value, AggregateStat digits) {
        this.value = value;
        this.digits = digits;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        final double offset = Math.pow(10, digits.apply(multiFTGSIterator));
        return Math.floor(value.apply(multiFTGSIterator) * offset) / offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateFloor that = (AggregateFloor) o;
        return Objects.equal(value, that.value) &&
                Objects.equal(digits, that.digits);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value, digits);
    }

    @Override
    public String toString() {
        return "AggregateFloor{" +
                "value=" + value +
                "digits=" + digits +
                '}';
    }

}
