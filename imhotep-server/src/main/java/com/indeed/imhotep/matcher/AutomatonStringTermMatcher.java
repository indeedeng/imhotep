package com.indeed.imhotep.matcher;

import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.imhotep.automaton.RegExp;
import com.indeed.imhotep.automaton.RunAutomaton;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

class AutomatonStringTermMatcher implements StringTermMatcher {
    private final RunAutomaton automaton;

    AutomatonStringTermMatcher(final String regex) {
        this.automaton = new RunAutomaton(new RegExp(regex).toAutomaton());
    }

    @Override
    public boolean matches(final String term) {
        return automaton.run(term);
    }

    @Override
    public boolean matches(final byte[] termBytes, final int termBytesLength) {
        return matches(new String(termBytes, 0, termBytesLength, StandardCharsets.UTF_8));
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        // TODO: Automaton#getCommonPrefix and reset termIterator.
        // TODO: Do something similar to what we do with stateStack in IncludeStringTermMatcher / SuffixStringTermMatcher.
        // TODO: Determine sink state and eagerly stop evaluating automaton.
        while (termIterator.next()) {
            final String term = termIterator.term();
            if (automaton.run(term)) {
                onMatch.accept(termIterator);
            }
        }
    }
}
