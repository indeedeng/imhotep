package com.indeed.imhotep.local;

import java.net.Socket;

import com.indeed.imhotep.io.SocketUtils;

public class NativeFTGSWorker {
    private final long nativeWorkerStructPtr;
    private final long nativeSessionStructPtr;
    
    public NativeFTGSWorker(int numGroups, int numMetrics, int numShards, Socket[] sockets, int id) {
        int[] socketArr = new int[sockets.length];

        for (int i = 0; i < sockets.length; i++) {
            Socket sock = sockets[i];
            int fd = SocketUtils.getOutputDescriptor(sock);
            socketArr[i] = fd;
        }
        nativeWorkerStructPtr = native_init(id, numMetrics, numGroups, socketArr, socketArr.length);
        nativeSessionStructPtr = native_session_create(numGroups, numMetrics, numShards);
    }
    
//    public void runTGSIteration(int term, long[] 

    private static native long native_session_create(int numGroups, int numMetrics, int numShards);

    private static native long native_init(int id,
                                           int numGroups,
                                           int numMetrics,
                                           int[] socketArr,
                                           int length);

}
