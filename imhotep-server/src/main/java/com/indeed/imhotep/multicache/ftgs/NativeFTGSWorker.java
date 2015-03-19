package com.indeed.imhotep.multicache.ftgs;

import java.net.Socket;

import com.indeed.imhotep.io.SocketUtils;
import com.indeed.imhotep.multicache.ProcessingTask;

public class NativeFTGSWorker extends ProcessingTask<NativeFtgsRunner.NativeTGSinfo,Void> {
    private long nativeWorkerStructPtr;
    private long nativeSessionStructPtr;
    private final int numGroups;
    private final int numMetrics;
    private final int numShards;
    private final int id;
    private long[] nativeMulticachePtrs;
    private final int[] socketArr;

    public NativeFTGSWorker(int numGroups,
                            int numMetrics,
                            int numShards,
                            Socket[] sockets,
                            int id,
                            long[] nativeMulticachePtrs) {
        this.numGroups = numGroups;
        this.numMetrics = numMetrics;
        this.numShards = numShards;
        this.id = id;
        this.nativeMulticachePtrs = nativeMulticachePtrs;
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
                                                       this.nativeMulticachePtrs,
                                                       numShards,
                                                       numGroups,
                                                       numMetrics);
    }

    @Override
    public Void processData(NativeFtgsRunner.NativeTGSinfo info) {
        int err;
        if (info.termDesc.isIntTerm) {
            err = native_run_int_tgs_pass(this.nativeWorkerStructPtr,
                                          this.nativeSessionStructPtr,
                                          info.termDesc.intTerm,
                                          info.termDesc.offsets,
                                          info.termDesc.numDocsInTerm,
                                          info.termDesc.size(),
                                          info.splitIndex);
        } else {
            err = native_run_string_tgs_pass(this.nativeWorkerStructPtr,
                                             this.nativeSessionStructPtr,
                                             info.termDesc.stringTerm,
                                             info.termDesc.stringTermLen,
                                             info.termDesc.offsets,
                                             info.termDesc.numDocsInTerm,
                                             info.termDesc.size(),
                                             info.splitIndex);
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
                                                         int numShards,
                                                         int splitIndex);

    private static native int native_run_int_tgs_pass(long nativeWorkerStructPtr,
                                                      long nativeSessionStructPtr,
                                                      long term,
                                                      long[] offsets,
                                                      int[] numDocsPerShard,
                                                      int numShards,
                                                      int splitIndex);

    private static native long native_session_create(long nativeWorkerStructPtr,
                                                     long[] nativeMulticachePtrs,
                                                     int numShards,
                                                     int numGroups,
                                                     int numMetrics);

    private static native void native_session_destroy(long nativeWorkerStructPtr,
                                                      long nativeSessionStructPtr);

    private static native long native_init(int id,
                                           int numGroups,
                                           int numMetrics,
                                           int[] socketArr,
                                           int length);

    private static native void native_worker_destroy(long nativeWorkerStructPtr);
}
