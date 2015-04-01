package com.indeed.imhotep.multicache.ftgs;

import java.net.Socket;
import java.nio.charset.Charset;

import com.indeed.imhotep.io.SocketUtils;
import com.indeed.imhotep.multicache.ProcessingTask;

public class NativeFTGSWorker extends ProcessingTask<NativeFtgsRunner.NativeTGSinfo,Void> {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private long nativeWorkerStructPtr;
    private long nativeSessionStructPtr;
    private final int numGroups;
    private final int numMetrics;
    private final int numShards;
    private final int id;
    private long[] nativeMulticachePtrs;
    private final int[] socketArr;
    private final String field;
    private final boolean isIntField;

    public NativeFTGSWorker(String field,
                            boolean isIntField,
                            int numGroups,
                            int numMetrics,
                            int numShards,
                            Socket[] sockets,
                            int id,
                            long[] nativeMulticachePtrs) {
        this.field = field;
        this.isIntField = isIntField;
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
        
        final byte[] fieldNameBytes = this.field.getBytes(UTF_8);
        native_start_field(nativeWorkerStructPtr,
                           nativeSessionStructPtr,
                           fieldNameBytes,
                           fieldNameBytes.length,
                           this.isIntField);
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
                                          info.socketNum);
        } else {
            err = native_run_string_tgs_pass(this.nativeWorkerStructPtr,
                                             this.nativeSessionStructPtr,
                                             info.termDesc.stringTerm,
                                             info.termDesc.stringTermLen,
                                             info.termDesc.offsets,
                                             info.termDesc.numDocsInTerm,
                                             info.termDesc.size(),
                                             info.socketNum);
        }

        return null;
    }

    @Override
    protected void cleanup() {
        native_end_field(this.nativeWorkerStructPtr, this.nativeSessionStructPtr);
        native_session_destroy(this.nativeWorkerStructPtr, this.nativeSessionStructPtr);
        native_worker_destroy(this.nativeWorkerStructPtr);
    }

    private static native int native_start_field(long nativeWorkerStructPtr,
                                                 long nativeSessionStructPtr,
                                                 byte[] field_bytes,
                                                 int length,
                                                 boolean isIntTerm);

    private static native int native_end_field(long nativeWorkerStructPtr,
                                               long nativeSessionStructPtr);

    private static native int native_run_string_tgs_pass(long nativeWorkerStructPtr,
                                                         long nativeSessionStructPtr,
                                                         byte[] stringTerm,
                                                         int stringTermLen,
                                                         long[] offsets,
                                                         int[] numDocsInTerm,
                                                         int numShards,
                                                         int socketNum);

    private static native int native_run_int_tgs_pass(long nativeWorkerStructPtr,
                                                      long nativeSessionStructPtr,
                                                      long term,
                                                      long[] offsets,
                                                      int[] numDocsPerShard,
                                                      int numShards,
                                                      int socketNum);

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
