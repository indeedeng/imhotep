package com.indeed.imhotep.multicache.ftgs;

import java.net.Socket;

import com.indeed.imhotep.io.SocketUtils;
import com.indeed.imhotep.multicache.ProcessingTask;

public class NativeFTGSWorker extends ProcessingTask<TermGroupStatOpDesc,Void> {
    private long nativeWorkerStructPtr;
    private long nativeSessionStructPtr;
    private final int numGroups;
    private final int numMetrics;
    private final int numShards;
    private final int id;
    private final int[] socketArr;

    public NativeFTGSWorker(int numGroups, int numMetrics, int numShards, Socket[] sockets, int id) {
        this.numGroups = numGroups;
        this.numMetrics = numMetrics;
        this.numShards = numShards;
        this.id = id;
        this.socketArr = new int[sockets.length];

        for (int i = 0; i < sockets.length; i++) {
            Socket sock = sockets[i];
            int fd = SocketUtils.getOutputDescriptor(sock);
            socketArr[i] = fd;
        }
    }

    @Override
    protected void init() {
        nativeWorkerStructPtr = native_init(id, numMetrics, numGroups, socketArr, socketArr.length);
        nativeSessionStructPtr = native_session_create(this.nativeWorkerStructPtr,
                                                       numGroups,
                                                       numMetrics,
                                                       numShards);
    }

    @Override
    public Void processData(TermGroupStatOpDesc termDescs) {
        int err;
        if (termDescs.isIntTerm) {
            err = native_run_int_tgs_pass(this.nativeWorkerStructPtr,
                                          this.nativeSessionStructPtr,
                                          termDescs.intTerm,
                                          termDescs.offsets,
                                          termDescs.numDocsInTerm,
                                          termDescs.shardIds,
                                          termDescs.size(),
                                          SocketUtils.getOutputDescriptor(termDescs.socket));
        } else {
            err = native_run_string_tgs_pass(this.nativeWorkerStructPtr,
                                             this.nativeSessionStructPtr,
                                             termDescs.stringTerm,
                                             termDescs.stringTermLen,
                                             termDescs.offsets,
                                             termDescs.numDocsInTerm,
                                             termDescs.shardIds,
                                             termDescs.size(),
                                             SocketUtils.getOutputDescriptor(termDescs.socket));
        }

        return null;
    }

    @Override
    protected void cleanup() {
        native_session_destroy(this.nativeWorkerStructPtr, this.nativeSessionStructPtr);
        native_worker_destroy(this.nativeWorkerStructPtr);
    }

    private static native int native_run_string_tgs_pass(long nativeWorkerStructPtr,
                                                         long nativeSessionStructPtr,
                                                         byte[] stringTerm,
                                                         int stringTermLen,
                                                         long[] offsets,
                                                         int[] numDocsInTerm,
                                                         int[] shardIds,
                                                         int numShards,
                                                         int socketFileDescriptor);

    private static native int native_run_int_tgs_pass(long nativeWorkerStructPtr,
                                                      long nativeSessionStructPtr,
                                                      int term,
                                                      long[] offsets,
                                                      int[] numDocsPerShard,
                                                      int[] shardIds,
                                                      int numShards,
                                                      int socketFileDescriptor);

    private static native long native_session_create(long nativeWorkerStructPtr,
                                                     int numGroups,
                                                     int numMetrics,
                                                     int numShards);

    private static native void native_session_destroy(long nativeWorkerStructPtr,
                                                      long nativeSessionStructPtr);

    private static native long native_init(int id,
                                           int numGroups,
                                           int numMetrics,
                                           int[] socketArr,
                                           int length);

    private static native void native_worker_destroy(long nativeWorkerStructPtr);
}
