package com.indeed.imhotep.service;

import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.client.ShardTimeUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/** For use by tests that need to generate random datasets */
class SkeletonDataset {

    private final Random rng;
    private final Path datasetDir;
    private final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private final String[] intFieldNames;
    private final String[] strFieldNames;
    private final int      numShards;

    SkeletonDataset(Random rng,
                    Path rootDir,
                    int maxNumShards,
                    int maxNumDocs,
                    int maxNumFields)
        throws IOException {

        this.rng           = rng;
        this.datasetDir    = Files.createTempDirectory(rootDir, "TestDataset-delete.me");
        this.intFieldNames = randomFieldNames(maxNumFields);
        this.strFieldNames = randomFieldNames(maxNumFields);
        this.numShards     = Math.max(rng.nextInt(maxNumShards), 1);

        for (int numShard = 0; numShard < numShards; ++numShard) {
            final String shardName = ShardTimeUtils.toHourlyShardPrefix(TODAY.minusHours(numShards).plusHours(numShard));
            final Path shardDir = datasetDir.resolve(shardName);
            final int numDocs = Math.max(rng.nextInt(maxNumDocs), 1);
            makeShard(shardDir, numDocs);
        }
    }

    Path        getDatasetDir() { return datasetDir;    }
    String[] getIntFieldNames() { return intFieldNames; }
    String[] getStrFieldNames() { return strFieldNames; }
    int          getNumShards() { return numShards;     }

    private void makeShard(Path shardDir, int maxNumDocs)
        throws IOException {
        final int numDocs = Math.max(rng.nextInt(maxNumDocs), 1);
        try (SimpleFlamdexWriter sflw = new SimpleFlamdexWriter(shardDir, numDocs)) {
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
        final int numFields       = Math.max(rng.nextInt(maxNumFields), 1);
        final     String[] result = new String[numFields];
        for (int idx = 0; idx < result.length; ++idx) {
            result[idx] = randomFieldName();
        }
        return result;
    }

    private String randomFieldName() {
        return RandomStringUtils.random(16, 0, 0, true, false, null, rng);
    }
}
