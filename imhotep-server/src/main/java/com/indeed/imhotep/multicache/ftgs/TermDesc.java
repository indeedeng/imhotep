package com.indeed.imhotep.multicache.ftgs;

/**
* Created by darren on 3/17/15.
*/
public final class TermDesc {
    public String field;
    public long nativeDocAddresses[];
    public int numDocsInTerm[];
    public int size;
    public boolean isIntTerm;
    public long intTerm;
    public byte[] stringTerm;
    public int stringTermLen;

    public TermDesc(int capacity) {
        this.nativeDocAddresses = new long[capacity];
        this.numDocsInTerm = new int[capacity];
        this.size = 0;
    }

    public void clear() {
        this.size = 0;
    }

    public int size() {
        return this.size;
    }
}
