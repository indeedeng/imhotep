package com.indeed.imhotep.local;

import java.net.Socket;

/**
* @author arun.
*/
final class FTGSIterateRequest {

    static final FTGSIterateRequest END = new FTGSIterateRequest(0);
    String field;
    byte[] stringTerm = new byte[100];
    int stringTermLength;
    long intTerm;
    boolean isIntField;
    final long[] offsets;
    long docsAddress;
    Socket outputSocket;

    FTGSIterateRequest(int numShards) {
        offsets = new long[numShards];
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setStringTerm(byte[] stringTerm, int stringTermLength) {
        if (stringTerm.length > this.stringTerm.length) {
            this.stringTerm = new byte[stringTerm.length * 2];
        }
        System.arraycopy(stringTerm, 0, this.stringTerm, 0, stringTermLength);
        this.stringTermLength = stringTermLength;
    }

    public void setIntTerm(long intTerm) {
        this.intTerm = intTerm;
    }

    public void setIntField(boolean isIntField) {
        this.isIntField = isIntField;
    }

    public void setOutputSocket(Socket outputSocket) {
        this.outputSocket = outputSocket;
    }

    public void setOffsets(final long[] offsets) {
        System.arraycopy(offsets, 0, this.offsets, 0, offsets.length);
    }
}
