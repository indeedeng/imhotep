package com.indeed.imhotep.matcher;

import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.imhotep.automaton.Automaton;
import com.indeed.imhotep.automaton.RegExp;

import java.util.function.Consumer;

class AutomatonStringTermMatcher extends StringTermMatcher {
    private final Automaton automaton;

    AutomatonStringTermMatcher(final String regex) {
        this.automaton = new RegExp(regex).toAutomaton();
    }

    @Override
    public boolean matches(final String term) {
        return automaton.run(term);
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        // TODO: Automaton#getCommonPrefix and reset termIterator.
        // TODO: Do something similar to stateStack.
        // TODO: Determine sink state and eagerly stop evaluating automaton.
        while (termIterator.next()) {
            final String term = termIterator.term();
            if (automaton.run(term)) {
                onMatch.accept(termIterator);
            }
        }
    }
}
