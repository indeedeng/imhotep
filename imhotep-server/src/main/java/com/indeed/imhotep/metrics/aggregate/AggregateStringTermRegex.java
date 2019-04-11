package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;
import com.indeed.imhotep.matcher.StringTermMatcher;
import com.indeed.imhotep.matcher.StringTermMatchers;

/**
 * @author jwolfe
 */
public class AggregateStringTermRegex implements AggregateStat {
    private final String regex;
    private final StringTermMatcher stringTermMatcher;

    public AggregateStringTermRegex(final String regex) {
        this.regex = regex;
        this.stringTermMatcher = StringTermMatchers.forRegex(regex);
    }

    @Override
    public double apply(final MultiFTGSIterator multiFTGSIterator) {
        return AggregateStat.floaty(stringTermMatcher.matches(multiFTGSIterator.termStringBytes(), multiFTGSIterator.termStringLength()));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if ((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        final AggregateStringTermRegex that = (AggregateStringTermRegex) o;
        return Objects.equal(regex, that.regex);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(regex);
    }

    @Override
    public String toString() {
        return "AggregateStringTermRegex{" +
                "regex=" + regex +
                '}';
    }
}
