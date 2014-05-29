package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.RawFTGSIterator;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.util.List;

/**
 * @author jsgroth
 */
public class FTGSFileBenchmark {
    static final String inputDir = "/home/jsgroth/ftgs";

    public static void main(String[] args) throws FileNotFoundException {
        for (int i = 0; i < 5; ++i) {
            runBenchmark();
        }
    }

    private static void runBenchmark() throws FileNotFoundException {
        List<RawFTGSIterator> iterators = Lists.newArrayList();
        for (File file : new File(inputDir).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".ftgs");
            }
        })) {
            iterators.add(new InputStreamFTGSIterator(new BufferedInputStream(new FileInputStream(file)), 4));
        }
        FTGSIterator merger = new RawFTGSMerger(iterators, 4, null);

        long elapsed = -System.currentTimeMillis();
        long[] stats = new long[4];
        while (merger.nextField()) {
            while (merger.nextTerm()) {
                while (merger.nextGroup()) {
                    merger.groupStats(stats);
                }
            }
        }
        elapsed += System.currentTimeMillis();
        System.out.println("time for iteration:"+elapsed+"ms");
    }
}
