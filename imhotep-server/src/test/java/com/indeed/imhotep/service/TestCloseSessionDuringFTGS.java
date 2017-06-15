/*
 * Copyright (C) 2014 Indeed Inc.
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

//import com.indeed.common.search.directory.MMapBufferDirectory;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.TestFileUtils;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestCloseSessionDuringFTGS {
    @BeforeClass
    public static void setMMapBufferDirectory() {
        // Use MMapBufferDirectory because it has the convenient behavior of segfaulting the JVM if you try to
        // read from a closed index
//        System.setProperty("org.apache.lucene.FSDirectory.class", MMapBufferDirectory.class.getName());
    }

    @Test
    public void testCloseSessionDuringFTGS() throws ImhotepOutOfMemoryException, IOException, InterruptedException {
        final Path shardDir = TestFileUtils.createTempShard();
        {
            final IndexWriter w = new IndexWriter(shardDir.toFile(), new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

            final Random rand = new Random();
            for (int i = 0; i < 1000000; ++i) {
                final int numTerms = rand.nextInt(5) + 5;
                final Document doc = new Document();
                for (int t = 0; t < numTerms; ++t) {
                    doc.add(new Field("sf1",
                                      Integer.toString(rand.nextInt(10000)),
                                      Field.Store.NO,
                                      Field.Index.NOT_ANALYZED_NO_NORMS));
                }
                w.addDocument(doc);
            }

            w.close();

            final AtomicBoolean closed = new AtomicBoolean(false);
            final FlamdexReader r = new LuceneFlamdexReader(shardDir) {
                @Override
                public void close() throws IOException {
                    super.close();
                    closed.set(true);
                }
            };
            final ImhotepSession session =
                new MTImhotepLocalMultiSession(new ImhotepLocalSession[] {
                        new ImhotepJavaLocalSession(r) },
                        new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                        null,
                        false
                );
//                FTGSIterator iter = session.getFTGSIterator(new String[]{}, new String[]{"sf1"}); //TODO fix this
            session.close();
            assertTrue(closed.get());
        }
    }
}
