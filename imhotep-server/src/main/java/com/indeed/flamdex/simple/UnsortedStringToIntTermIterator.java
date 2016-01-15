package com.indeed.flamdex.simple;

import java.nio.file.Path;

/**
 * @author vladimir
 */

public class UnsortedStringToIntTermIterator implements SimpleIntTermIterator  {
    private final SimpleStringTermIterator stringTermIterator;

    public UnsortedStringToIntTermIterator(SimpleStringTermIterator stringTermIterator) {
        this.stringTermIterator = stringTermIterator;
    }

    @Override
    public void reset(long term) {
        stringTermIterator.reset(String.valueOf(term));
    }

    @Override
    public long term() {
        try {
            return Long.parseLong(stringTermIterator.term());
        } catch(NumberFormatException ignored) {
            return 0;
        }
    }

    @Override
    public Path getFilename() {
        return stringTermIterator.getFilename();
    }

    @Override
    public long getOffset() {
        return stringTermIterator.getOffset();
    }

    @Override
    public boolean next() {
        return stringTermIterator.next();
    }

    @Override
    public int docFreq() {
        return stringTermIterator.docFreq();
    }

    @Override
    public void close() {
        stringTermIterator.close();
    }

    @Override
    public long getDocListAddress()
        throws java.io.IOException {
        return stringTermIterator.getDocListAddress();
    }
}
