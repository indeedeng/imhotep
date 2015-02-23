package com.indeed.imhotep.local;

import java.net.Socket;

import com.indeed.imhotep.io.SocketUtils;

public class NativeFTGSWorker {
    private final long nativeWorkerStructPtr;
    private final long nativeSessionStructPtr;

    public static final class TermDescList {
        public int shardIds[];
        public long offsets[];
        public int numDocsInTerm[];
        private int size;

        public TermDescList(int capacity) {
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
    
    public NativeFTGSWorker(int numGroups, int numMetrics, int numShards, Socket[] sockets, int id) {
        int[] socketArr = new int[sockets.length];

        for (int i = 0; i < sockets.length; i++) {
            Socket sock = sockets[i];
            int fd = SocketUtils.getOutputDescriptor(sock);
            socketArr[i] = fd;
        }
        nativeWorkerStructPtr = native_init(id, numMetrics, numGroups, socketArr, socketArr.length);
        nativeSessionStructPtr = native_session_create(this.nativeWorkerStructPtr,
                                                       numGroups,
                                                       numMetrics,
                                                       numShards);
    }

    public void runIntFieldTGSIteration(int term, TermDescList termDescs, Socket socket) {
        int err = native_run_int_tgs_pass(this.nativeWorkerStructPtr,
                                          this.nativeSessionStructPtr,
                                          term,
                                          termDescs.offsets,
                                          termDescs.numDocsInTerm,
                                          termDescs.shardIds,
                                          termDescs.size(),
                                          SocketUtils.getOutputDescriptor(socket));
    }

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

    private static native long native_init(int id,
                                           int numGroups,
                                           int numMetrics,
                                           int[] socketArr,
                                           int length);

}
