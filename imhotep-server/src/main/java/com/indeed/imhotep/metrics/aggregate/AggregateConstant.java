package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

/**
 * @author jwolfe
 */
public class AggregateConstant implements AggregateStat {
    private final double value;

    public AggregateConstant(double value) {
        this.value = value;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateConstant that = (AggregateConstant) o;
        return Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return "AggregateConstant{" +
                "value=" + value +
                '}';
    }
}
