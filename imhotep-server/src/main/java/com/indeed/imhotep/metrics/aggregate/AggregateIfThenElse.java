package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

/**
 * @author jwolfe
 */
public class AggregateIfThenElse implements AggregateStat {
    private final AggregateStat condition;
    private final AggregateStat trueCase;
    private final AggregateStat falseCase;

    public AggregateIfThenElse(AggregateStat condition, AggregateStat trueCase, AggregateStat falseCase) {
        this.condition = condition;
        this.trueCase = trueCase;
        this.falseCase = falseCase;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        if (AggregateStat.truthy(condition.apply(multiFTGSIterator))) {
            return trueCase.apply(multiFTGSIterator);
        } else {
            return falseCase.apply(multiFTGSIterator);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateIfThenElse that = (AggregateIfThenElse) o;
        return Objects.equal(condition, that.condition) &&
                Objects.equal(trueCase, that.trueCase) &&
                Objects.equal(falseCase, that.falseCase);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(condition, trueCase, falseCase);
    }

    @Override
    public String toString() {
        return "AggregateIfThenElse{" +
                "condition=" + condition +
                ", trueCase=" + trueCase +
                ", falseCase=" + falseCase +
                '}';
    }
}
