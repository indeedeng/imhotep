package com.indeed.imhotep.local;

import java.net.Socket;

/**
 * @author arun.
 */
interface MultiShardFTGSExecutor {
    //TODO: these methods should also take the handle that that future push stat will return
    public void writeFTGSSplitForIntTerm(String field, long term, long[] offsets, Socket socket);
    public void writeFTGSSplitForStringTerm(String field, byte[] stringTermBytes, int termLength, long[] offsets, Socket socket);
}
