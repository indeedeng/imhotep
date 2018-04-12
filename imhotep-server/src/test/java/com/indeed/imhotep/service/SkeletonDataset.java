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

    SkeletonDataset(final Random rng,
                    final Path rootDir,
                    final int maxNumShards,
                    final int maxNumDocs,
                    final int maxNumFields)
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

    private void makeShard(final Path shardDir, final int maxNumDocs)
        throws IOException {
        final int numDocs = Math.max(rng.nextInt(maxNumDocs), 1);
        try (SimpleFlamdexWriter sflw = new SimpleFlamdexWriter(shardDir, numDocs)) {
                for (final String field: intFieldNames) {
                    IntFieldWriter ifw = null;
                    try {
                        ifw = sflw.getIntFieldWriter(field);
                    }
                    finally {
                        if (ifw != null) {
                            ifw.close();
                        }
                    }
                }
                for (final String field: strFieldNames) {
                    StringFieldWriter sfw = null;
                    try {
                        sfw = sflw.getStringFieldWriter(field);
                    }
                    finally {
                        if (sfw != null) {
                            sfw.close();
                        }
                    }
                }
            }
    }

    private String[] randomFieldNames(final int maxNumFields) {
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
