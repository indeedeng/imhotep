package com.indeed.imhotep.multicache.ftgs;

import java.util.Arrays;

/**
* Created by darren on 3/17/15.
*/
public final class TermDesc {
    public String field;
    public long nativeDocAddresses[];
    public int numDocsInTerm[];
    public int validShardCount;
    public boolean isIntTerm;
    public long intTerm;
    public byte[] stringTerm;
    public int stringTermLen;
    public int sz;

    public TermDesc(int capacity) {
        this.nativeDocAddresses = new long[capacity];
        this.numDocsInTerm = new int[capacity];
        this.validShardCount = 0;
        this.sz = capacity;
    }

    public void clear() {
        Arrays.fill(this.nativeDocAddresses, 0L);
        Arrays.fill(this.numDocsInTerm, 0);
        this.validShardCount = 0;
        this.intTerm = -1;
        this.field = null;
        this.stringTerm = null;
        this.stringTermLen = 0;
    }

    public int size() {
        return this.sz;
    }
}
