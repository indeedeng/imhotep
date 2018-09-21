package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

import java.util.Arrays;

/**
 * @author jwolfe
 */
public class AggregatePerGroupConstant implements AggregateStat {
    private final double[] values;

    public AggregatePerGroupConstant(double[] values) {
        this.values = values;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return values[multiFTGSIterator.group()];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregatePerGroupConstant that = (AggregatePerGroupConstant) o;
        return Objects.equal(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(values);
    }

    @Override
    public String toString() {
        return "AggregatePerGroupConstant{" +
                "values=" + Arrays.toString(values) +
                '}';
    }
}
