package com.indeed.imhotep.multicache.ftgs;

import java.net.Socket;
import java.nio.charset.Charset;

import com.indeed.imhotep.io.SocketUtils;
import com.indeed.imhotep.multicache.ProcessingTask;

public class NativeFTGSWorker extends ProcessingTask<NativeFTGSOpIterator.NativeTGSinfo,Void> {
    private static final Charset UTF_8 = Charset.forName("UTF-8");

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

    private void sendStartField(String field, boolean isIntField, int socketNum) {
        final byte[] fieldNameBytes = field.getBytes(UTF_8);
        native_start_field(nativeWorkerStructPtr,
                           nativeSessionStructPtr,
                           fieldNameBytes,
                           fieldNameBytes.length,
                           isIntField,
                           socketNum);
    }

    private void sendEndField(boolean isIntField, int socketNum) {
        native_end_field(this.nativeWorkerStructPtr, this.nativeSessionStructPtr, socketNum);
    }

    private void sendNoMoreFields(boolean isIntField, int socketNum) {
        native_end_stream(this.nativeWorkerStructPtr, this.nativeSessionStructPtr, socketNum);
    }

    @Override
    public Void processData(NativeFTGSOpIterator.NativeTGSinfo info) {
        switch (info.operation) {
            case NativeFTGSOpIterator.NativeTGSinfo.FIELD_START_OPERATION:
                sendStartField(info.fieldName, info.isIntField, info.socketNum);
                break;
            case NativeFTGSOpIterator.NativeTGSinfo.TGS_OPERATION:
                processTerm(info);
                break;
            case NativeFTGSOpIterator.NativeTGSinfo.FIELD_END_OPERATION:
                sendEndField(info.isIntField, info.socketNum);
                break;
            case NativeFTGSOpIterator.NativeTGSinfo.NO_MORE_FIELDS_OPERATION:
                sendNoMoreFields(info.isIntField, info.socketNum);
                break;

        }

        return null;
    }


    private void processTerm(NativeFTGSOpIterator.NativeTGSinfo info) {
        int err;

        if (info.termDesc.isIntTerm) {
            err = native_run_int_tgs_pass(this.nativeWorkerStructPtr,
                                          this.nativeSessionStructPtr,
                                          info.termDesc.intTerm,
                                          info.termDesc.nativeDocAddresses,
                                          info.termDesc.numDocsInTerm,
                                          info.termDesc.size(),
                                          info.socketNum);
        } else {
            err = native_run_string_tgs_pass(this.nativeWorkerStructPtr,
                                             this.nativeSessionStructPtr,
                                             info.termDesc.stringTerm,
                                             info.termDesc.stringTermLen,
                                             info.termDesc.nativeDocAddresses,
                                             info.termDesc.numDocsInTerm,
                                             info.termDesc.size(),
                                             info.socketNum);
        }
    }

    @Override
    protected void complete() { }

    @Override
    protected void cleanup() {
        if (this.nativeSessionStructPtr != 0) {
            native_session_destroy(this.nativeWorkerStructPtr, this.nativeSessionStructPtr);
        }
        if (this.nativeWorkerStructPtr != 0) {
            native_worker_destroy(this.nativeWorkerStructPtr);
        }
    }

    private static native int native_start_field(long nativeWorkerStructPtr,
                                                 long nativeSessionStructPtr,
                                                 byte[] field_bytes,
                                                 int length,
                                                 boolean isIntTerm,
                                                 int socketNum);

    private static native int native_end_field(long nativeWorkerStructPtr,
                                               long nativeSessionStructPtr,
                                               int socketNum);

    private static native int native_end_stream(long nativeWorkerStructPtr,
                                                long nativeSessionStructPtr,
                                                int socketNum);

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
