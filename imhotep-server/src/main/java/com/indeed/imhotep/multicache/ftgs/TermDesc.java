package com.indeed.imhotep.multicache.ftgs;

import com.indeed.flamdex.datastruct.CopyingBlockingQueue;

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

    public static void copy(TermDesc dest, TermDesc src) {
        dest.field = src.field;
        System.arraycopy(src.nativeDocAddresses, 0, dest.nativeDocAddresses, 0, src.size());
        System.arraycopy(src.numDocsInTerm, 0, dest.numDocsInTerm, 0, src.size());
        dest.validShardCount = src.validShardCount;
        dest.isIntTerm = src.isIntTerm;
        dest.intTerm = src.intTerm;
        if (src.stringTerm == null) {
            dest.stringTerm = null;
        } else {
            if (dest.stringTerm.length < src.stringTermLen) {
                dest.stringTerm = Arrays.copyOf(src.stringTerm, src.stringTermLen);
            } else {
                System.arraycopy(src.stringTerm, 0, dest.stringTerm, 0, src.stringTermLen);
            }

        }
        dest.stringTermLen = src.stringTermLen;
        // don't copy sz
    }

}
