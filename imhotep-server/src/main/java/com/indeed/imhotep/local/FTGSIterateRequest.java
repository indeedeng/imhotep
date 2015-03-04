package com.indeed.imhotep.local;

import java.net.Socket;

/**
* @author arun.
*/
final class FTGSIterateRequest {
    static final FTGSIterateRequest END = new FTGSIterateRequest("", new byte[]{}, -1, true, new long[]{}, new Socket());
    final String field;
    final byte[] stringTerm;
    final long intTerm;
    final boolean isIntField;
    final long[] offsets;
    final Socket outputSocket;

    private FTGSIterateRequest(String field, byte[] stringTerm, long intTerm, boolean isIntField, long[] offsets, Socket outputSocket) {
        this.field = field;
        this.stringTerm = stringTerm;
        this.intTerm = intTerm;
        this.isIntField = isIntField;
        //noinspection AssignmentToCollectionOrArrayFieldFromParameter
        this.offsets = offsets;
        this.outputSocket = outputSocket;
    }

    static FTGSIterateRequest create(final String field, final byte[] term, final long[] offsets, final Socket socket) {
        return new FTGSIterateRequest(field, term, -1, false, offsets, socket);
    }

    static FTGSIterateRequest create(final String field, final long term, final long[] offsets, final Socket socket) {
        return new FTGSIterateRequest(field, new byte[]{}, term, true, offsets, socket);
    }
}
