package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class AggregateCeil implements AggregateStat{
    private final AggregateStat value;
    private final int digits;
    private final double offset;

    public AggregateCeil(AggregateStat value, AggregateStat digits) {
        Preconditions.checkArgument(digits instanceof AggregateConstant);
        this.value = value;
        this.digits = (int)((AggregateConstant) digits).getValue();
        offset = Math.pow(10, this.digits);
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return Math.ceil(value.apply(multiFTGSIterator) * offset) / offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateCeil that = (AggregateCeil) o;
        return Objects.equal(value, that.value) &&
                digits == that.digits;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this.getClass().getName(), value, digits);
    }

    @Override
    public String toString() {
        return "AggregateCeil{" +
                "value=" + value +
                "digits=" + digits +
                '}';
    }

}
