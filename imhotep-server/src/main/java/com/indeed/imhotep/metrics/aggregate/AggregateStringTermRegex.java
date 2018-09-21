package com.indeed.imhotep.metrics.aggregate;

import com.google.common.base.Objects;
import com.indeed.imhotep.automaton.Automaton;
import com.indeed.imhotep.automaton.RegExp;

/**
 * @author jwolfe
 */
public class AggregateStringTermRegex implements AggregateStat {
    private final Automaton automaton;

    public AggregateStringTermRegex(String regex) {
        automaton = new RegExp(regex).toAutomaton();
    }

    @Override
    public double apply(MultiFTGSIterator multiFTGSIterator) {
        return AggregateStat.floaty(automaton.run(multiFTGSIterator.termStringVal()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateStringTermRegex that = (AggregateStringTermRegex) o;
        return Objects.equal(automaton, that.automaton);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(automaton);
    }

    @Override
    public String toString() {
        return "AggregateStringTermRegex{" +
                "automaton=" + automaton +
                '}';
    }
}
