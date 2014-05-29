package com.indeed.imhotep.service;

//import com.indeed.common.search.directory.MMapBufferDirectory;
import com.indeed.util.io.Files;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.lucene.LuceneFlamdexReader;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
        String tempDir = Files.getTempDirectory("asdf", "");
        try {
            IndexWriter w = new IndexWriter(tempDir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

            Random rand = new Random();
            for (int i = 0; i < 1000000; ++i) {
                int numTerms = rand.nextInt(5) + 5;
                Document doc = new Document();
                for (int t = 0; t < numTerms; ++t) {
                    doc.add(new Field("sf1", Integer.toString(rand.nextInt(10000)), Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
                }
                w.addDocument(doc);
            }

            w.close();

            final AtomicBoolean closed = new AtomicBoolean(false);
            FlamdexReader r = new LuceneFlamdexReader(IndexReader.open(tempDir)) {
                @Override
                public void close() throws IOException {
                    super.close();
                    closed.set(true);
                }
            };
            final ExecutorService executor = Executors.newCachedThreadPool();
            try {
                ImhotepSession session =
                        new MTImhotepMultiSession(new ImhotepLocalSession[] { new ImhotepLocalSession(r) },
                                                  new MemoryReservationContext(
                                                                               new ImhotepMemoryPool(
                                                                                                     Long.MAX_VALUE)),
                                                  executor, 8);
                FTGSIterator iter = session.getFTGSIterator(new String[]{}, new String[]{"sf1"});
                session.close();
                assertTrue(closed.get());
            } finally {
                executor.shutdown();
            }
        } finally {
            Files.delete(tempDir);
        }
    }
}
