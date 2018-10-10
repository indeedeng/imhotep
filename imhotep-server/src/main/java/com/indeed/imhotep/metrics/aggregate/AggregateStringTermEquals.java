package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;

import static com.indeed.imhotep.metrics.aggregate.AggregateStat.floaty;

/**
 * @author jwolfe
 */
public class AggregateStringTermEquals implements AggregateStat {
    private final String term;

    public AggregateStringTermEquals(String term) {
        this.term = term;
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return floaty(term.equals(multiFTGSIterator.termStringVal()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateStringTermEquals that = (AggregateStringTermEquals) o;
        return Objects.equal(term, that.term);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(term);
    }

    @Override
    public String toString() {
        return "AggregateStringTermEquals{" +
                "term='" + term + '\'' +
                '}';
    }
}
