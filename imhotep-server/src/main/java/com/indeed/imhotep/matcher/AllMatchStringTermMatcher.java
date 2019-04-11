package com.indeed.imhotep.matcher;

import com.indeed.flamdex.api.StringTermIterator;

import java.util.function.Consumer;

class AllMatchStringTermMatcher implements StringTermMatcher {
    @Override
    public boolean matches(final String term) {
        return true;
    }

    @Override
    public boolean matches(final byte[] termBytes, final int termBytesLength) {
        return true;
    }

    @Override
    public void run(final StringTermIterator termIterator, final Consumer<StringTermIterator> onMatch) {
        while (termIterator.next()) {
            onMatch.accept(termIterator);
        }
    }
}
