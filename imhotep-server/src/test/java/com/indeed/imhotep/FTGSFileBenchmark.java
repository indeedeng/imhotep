/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep;

import com.google.common.collect.Lists;
import com.indeed.imhotep.api.FTGSIterator;

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
    private FTGSFileBenchmark() {
    }

    private static final String inputDir = "/home/jsgroth/ftgs";

    public static void main(final String[] args) throws FileNotFoundException {
        for (int i = 0; i < 5; ++i) {
            runBenchmark();
        }
    }

    private static void runBenchmark() throws FileNotFoundException {
        final List<FTGSIterator> iterators = Lists.newArrayList();
        for (final File file : new File(inputDir).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.endsWith(".ftgs");
            }
        })) {
            iterators.add(new InputStreamFTGSIterator(new BufferedInputStream(new FileInputStream(file)), 4, Integer.MAX_VALUE));
        }
        final FTGSIterator merger = new FTGSMerger(iterators, null);

        long elapsed = -System.currentTimeMillis();
        final long[] stats = new long[4];
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
