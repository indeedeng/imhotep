package com.indeed.flamdex.simple;

/**
 * @author jsgroth
 */
final class NullStringTermIterator implements SimpleStringTermIterator {

    private static final byte[] EMPTY = new byte[0];

    private final String docsFilename;

    NullStringTermIterator(String docsFilename) {
        this.docsFilename = docsFilename;
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
    public void reset(String term) {
    }

    @Override
    public String term() {
        return "";
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

    @Override
    public byte[] termStringBytes() {
        return EMPTY;
    }

    @Override
    public int termStringLength() {
        return 0;
    }
}
