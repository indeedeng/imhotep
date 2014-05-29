package com.indeed.flamdex.simple;

/**
 * @author jsgroth
 */
final class NullIntTermIterator implements SimpleIntTermIterator {
    private final String docsFilename;

    NullIntTermIterator(String docsFilename) {
        this.docsFilename = docsFilename;
    }

    @Override
    public void reset(long term) {
    }

    @Override
    public long term() {
        return 0;
    }

    @Override
    public String getFilename() {
        return docsFilename;
    }

    @Override
    public long getOffset() {
        return 0L;
    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public int docFreq() {
        return 0;
    }

    @Override
    public void close() {
    }
}
