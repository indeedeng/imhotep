package com.indeed.imhotep.tools;

import com.google.common.io.ByteStreams;
import com.indeed.imhotep.FTGSMerger;
import com.indeed.imhotep.InputStreamFTGSIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.io.Streams;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jsgroth
 */
public class FTGSMergerBenchmark {
    private static final String inputDir = "/home/jsgroth/ftgsmerger";

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < 5; ++i) {
            final List<InputStream> inputStreams = new ArrayList<InputStream>();
            System.out.println("opening files and getting them in disk cache");
            for (final File file : new File(inputDir).listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    return pathname.getName().endsWith(".ftgs");
                }
            })) {
                // cat the file into /dev/null so it's in disk cache
                final InputStream is = Streams.newBufferedInputStream(new FileInputStream(file));
                ByteStreams.copy(is, ByteStreams.nullOutputStream());
                is.close();

                inputStreams.add(Streams.newBufferedInputStream(new FileInputStream(file)));
            }

            System.out.println("creating iterators");
            final List<FTGSIterator> iterators = new ArrayList<FTGSIterator>();
            for (final InputStream is : inputStreams) {
                iterators.add(new InputStreamFTGSIterator(is, 2));
            }
            final FTGSMerger merger = new FTGSMerger(iterators, 2, null);
            final long[] statBuf = new long[2];

            System.out.println("starting merge");
            long elapsed = -System.currentTimeMillis();
            while (merger.nextField()) {
                while (merger.nextTerm()) {
                    while (merger.nextGroup()) {
                        merger.groupStats(statBuf);
                    }
                }
            }
            elapsed += System.currentTimeMillis();
            System.out.println("time to exhaust iterators: "+elapsed+" ms");

            for (final InputStream is : inputStreams) {
                is.close();
            }
        }
    }
}
