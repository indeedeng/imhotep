package com.indeed.imhotep.multicache.ftgs;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.imhotep.local.MultiCache;
import com.indeed.imhotep.multicache.ProcessingService;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by darren on 3/18/15.
 */
public class NativeFTGSRunner {
    private static final Logger log = Logger.getLogger(NativeFTGSRunner.class);
    private static final int NUM_WORKERS = 8;
    private final long[] multicacheAddrs;
    private final int numGroups;
    private final int numMetrics;
    private final int numShards;
    private final NativeFTGSOpIterator iterator;

    public NativeFTGSRunner(final FlamdexReader[] flamdexReaders,
                            final MultiCache[] multiCaches,
                            final String[] intFields,
                            final String[] stringFields,
                            final int numGroups,
                            final int numMetrics,
                            final int numSplits) throws IOException {
        this.numGroups = numGroups;
        this.numMetrics = numMetrics;
        this.numShards = flamdexReaders.length;
        this.multicacheAddrs = new long[multiCaches.length];
        for (int i = 0; i < multiCaches.length; i++) {
            this.multicacheAddrs[i] = multiCaches[i].getNativeAddress();
        }

        this.iterator = new NativeFTGSOpIterator(flamdexReaders, intFields, stringFields, numSplits, NUM_WORKERS);
    }

    public void run(final Socket[] sockets,
                    final int nSockets,
                    final ExecutorService threadPool) {
        final ProcessingService.TaskCoordinator<NativeFTGSOpIterator.NativeTGSinfo> router;
        router = new ProcessingService.TaskCoordinator<NativeFTGSOpIterator.NativeTGSinfo>() {
            @Override
            public int route(NativeFTGSOpIterator.NativeTGSinfo info) {
                return info.splitIndex % NUM_WORKERS;
            }
        };
        final ProcessingService<NativeFTGSOpIterator.NativeTGSinfo, Void> service;
        service = new ProcessingService<>(router, threadPool);

        for (int i = 0; i < NUM_WORKERS; i++) {
            final List<Socket> socketList = new ArrayList<>();
            for (int j = 0; j < nSockets; j++) {
                if (j % NUM_WORKERS == i) {
                    socketList.add(sockets[j]);
                }
            }
            final Socket[] mySockets = socketList.toArray(new Socket[socketList.size()]);
            service.addTask(new NativeFTGSWorker(numGroups,
                                                 numMetrics,
                                                 numShards,
                                                 mySockets,
                                                 i,
                                                 multicacheAddrs));
        }

        service.processData(iterator, null);
    }
}
