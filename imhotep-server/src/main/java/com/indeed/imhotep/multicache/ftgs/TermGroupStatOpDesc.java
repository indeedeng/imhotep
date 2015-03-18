package com.indeed.imhotep.multicache.ftgs;

import java.net.Socket;

/**
* Created by darren on 3/17/15.
*/
public final class TermGroupStatOpDesc {
    public int shardIds[];
    public long offsets[];
    public int numDocsInTerm[];
    public int size;
    public boolean isIntTerm;
    public long intTerm;
    public byte[] stringTerm;
    public int stringTermLen;
    public Socket socket;

    public TermGroupStatOpDesc(int capacity) {
        this.shardIds = new int[capacity];
        this.offsets = new long[capacity];
        this.numDocsInTerm = new int[capacity];
        this.size = 0;
    }

    public void add(int id, long offset, int nDocs) {
        final int loc = size;

        shardIds[loc] = id;
        offsets[loc] = offset;
        numDocsInTerm[loc] = nDocs;
        size++;
    }

    public void clear() {
        this.size = 0;
    }

    public int size() {
        return this.size;
    }
}
