package com.indeed.imhotep;

import com.indeed.imhotep.api.FTGSIterator;

public class EmptyFTGSIterator implements FTGSIterator {
    private final String[] intFields;
    private final String[] strFields;
    private final int numStats;

    private int fieldIndex;
    private boolean isIntField;

    public EmptyFTGSIterator(final String[] intFields, final String[] strFields, final int numStats) {
        this.intFields = intFields;
        this.strFields = strFields;
        this.numStats = numStats;
        isIntField = true;
        fieldIndex = -1;
    }

    @Override
    public int getNumStats() {
        return numStats;
    }

    @Override
    public int getNumGroups() {
        return 0;
    }

    @Override
    public boolean nextField() {
        fieldIndex++;

        if (isIntField && (fieldIndex == intFields.length)) {
            fieldIndex = 0;
            isIntField = false;
        }
        return fieldIndex < (isIntField ? intFields : strFields).length;
    }

    @Override
    public String fieldName() {
        return (isIntField ? intFields : strFields)[fieldIndex];
    }

    @Override
    public boolean fieldIsIntType() {
        return isIntField;
    }

    @Override
    public boolean nextTerm() {
        return false;
    }

    @Override
    public long termDocFreq() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public long termIntVal() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public String termStringVal() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public byte[] termStringBytes() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public int termStringLength() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public boolean nextGroup() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public int group() {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public void groupStats(final long[] stats, final int offset) {
        throw new IllegalStateException("Should not call this method!");
    }

    @Override
    public void close() {
    }
}
