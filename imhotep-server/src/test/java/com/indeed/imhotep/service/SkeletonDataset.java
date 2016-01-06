package com.indeed.imhotep.service;

import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.util.io.Files;

import org.apache.commons.lang.RandomStringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/** For use by tests that need to generate random datasets */
class SkeletonDataset {

    private final Random rng;
    private final String datasetDir;

    private final String[] intFieldNames;
    private final String[] strFieldNames;
    private final int      numShards;

    SkeletonDataset(Random rng,
                    File rootDir,
                    int maxNumShards,
                    int maxNumDocs,
                    int maxNumFields)
        throws IOException {

        this.rng           = rng;
        this.datasetDir    = Files.getTempDirectory("TestDataset.", ".delete.me", rootDir);
        this.intFieldNames = randomFieldNames(maxNumFields);
        this.strFieldNames = randomFieldNames(maxNumFields);
        this.numShards     = rng.nextInt(maxNumShards) + 1;

        for (int numShard = 0; numShard < numShards; ++numShard) {
            final String shardName = String.format("index.xx.%14d", numShard);
            final File shardDir = new File(datasetDir, shardName);
            final int numDocs = rng.nextInt(maxNumDocs - 1) + 1;
            makeShard(shardDir, numDocs);
        }
    }

    String      getDatasetDir() { return datasetDir;    }
    String[] getIntFieldNames() { return intFieldNames; }
    String[] getStrFieldNames() { return strFieldNames; }
    int          getNumShards() { return numShards;     }

    private void makeShard(File shardDir, int maxNumDocs)
        throws IOException {
        final int numDocs = rng.nextInt(maxNumDocs - 1) + 1;
        try (SimpleFlamdexWriter sflw = new SimpleFlamdexWriter(shardDir.getCanonicalPath(), numDocs)) {
                for (String field: intFieldNames) {
                    IntFieldWriter ifw = null;
                    try {
                        ifw = sflw.getIntFieldWriter(field);
                    }
                    finally {
                        if (ifw != null) ifw.close();
                    }
                }
                for (String field: strFieldNames) {
                    StringFieldWriter sfw = null;
                    try {
                        sfw = sflw.getStringFieldWriter(field);
                    }
                    finally {
                        if (sfw != null) sfw.close();
                    }
                }
            }
    }

    private String[] randomFieldNames(int maxNumFields) {
        final int numFields       = rng.nextInt(maxNumFields) + 1;
        final     String[] result = new String[numFields];
        for (int idx = 0; idx < result.length; ++idx) {
            result[idx] = randomFieldName();
        }
        return result;
    }

    private String randomFieldName() {
        final int length = rng.nextInt(64) + 1;
        return RandomStringUtils.random(length, 0, 0, true, false, null, rng);
    }
}
