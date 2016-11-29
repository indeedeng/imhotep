package com.indeed.flamdex.dynamic;

import com.google.common.collect.ImmutableSet;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author michihiko
 */
public class TestDynamicFlamdexWriter {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int NUM_DOCS = (2 * 3 * 5 * 7 * 11) + 10;
    private Path directory;

    private int restoreNum(@Nonnull final FlamdexReader flamdexReader, final int docId) throws FlamdexOutOfMemoryException {
        final long[] num = new long[1];
        flamdexReader.getMetric("original").lookup(new int[]{docId}, num, 1);
        assertTrue((Integer.MIN_VALUE <= num[0]) && (num[0] <= Integer.MAX_VALUE));
        return (int) num[0];
    }

    @Nonnull
    private FlamdexDocument makeDocument(final int n) {
        final FlamdexDocument.Builder builder = new FlamdexDocument.Builder();
        builder.addIntTerm("original", n);
        builder.addIntTerm("mod2i", n % 2);
        builder.addIntTerm("mod3i", n % 3);
        builder.addStringTerm("mod5s", Integer.toString(n % 5));
        builder.addIntTerms("mod7mod11i", n % 7, n % 11);
        builder.addStringTerms("mod7mod11s", Integer.toString(n % 7), Integer.toString(n % 11));
        if (((n % 3) != 0) && ((n % 5) != 0)) {
            builder.addIntTerms("mod3mod5i_nonzero", n % 3, n % 5);
        }
        return builder.build();
    }

    @Before
    public void setUp() throws IOException {
        directory = temporaryFolder.getRoot().toPath();
    }

    @Test
    public void testSimpleStats() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(makeDocument(i));
        }
        Collections.shuffle(documents);

        final Random random = new Random(0);
        try (final DynamicFlamdexDocWriter flamdexDocWriter = new DynamicFlamdexDocWriter(directory)) {
            for (final FlamdexDocument document : documents) {
                flamdexDocWriter.addDocument(document);
                if (random.nextInt(200) == 0) {
                    flamdexDocWriter.flush(random.nextBoolean());
                }
            }
        }
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(directory)) {
            assertEquals(NUM_DOCS, flamdexReader.getNumDocs());
            assertEquals(NUM_DOCS, flamdexReader.getIntTotalDocFreq("original"));
            int numMod7Mod11i = 0;
            for (int i = 0; i < NUM_DOCS; ++i) {
                numMod7Mod11i += ((i % 7) == (i % 11)) ? 1 : 2;
            }
            assertEquals(numMod7Mod11i, flamdexReader.getIntTotalDocFreq("mod7mod11i"));
            assertEquals(
                    ImmutableSet.of("original", "mod2i", "mod3i", "mod7mod11i", "mod3mod5i_nonzero"),
                    ImmutableSet.copyOf(flamdexReader.getAvailableMetrics()));
            assertEquals(
                    ImmutableSet.of("original", "mod2i", "mod3i", "mod7mod11i", "mod3mod5i_nonzero"),
                    ImmutableSet.copyOf(flamdexReader.getIntFields()));
            assertEquals(
                    ImmutableSet.of("mod5s", "mod7mod11s"),
                    ImmutableSet.copyOf(flamdexReader.getStringFields()));
        }
    }

    private void addDocument(@Nonnull final FlamdexDocWriter writer, @Nonnull final Set<FlamdexDocument> naive, @Nonnull final FlamdexDocument doc) throws IOException {
        writer.addDocument(doc);
        naive.add(doc);
    }

    private void removeDocument(@Nonnull final DynamicFlamdexDocWriter writer, @Nonnull final Set<FlamdexDocument> naive, @Nonnull final String field, final long term) throws IOException {
        writer.deleteDocuments(Query.newTermQuery(Term.intTerm(field, term)));
        for (final Iterator<FlamdexDocument> iter = naive.iterator(); iter.hasNext(); ) {
            final FlamdexDocument flamdexDocument = iter.next();
            final LongList terms = flamdexDocument.getIntTerms(field);
            if (terms == null) {
                continue;
            }
            for (final Long value : terms) {
                if (value == term) {
                    iter.remove();
                    break;
                }
            }
        }
    }

    private void removeDocument(@Nonnull final DynamicFlamdexDocWriter writer, @Nonnull final Set<FlamdexDocument> naive, @Nonnull final String field, final String term) throws IOException {
        writer.deleteDocuments(Query.newTermQuery(Term.stringTerm(field, term)));
        for (final Iterator<FlamdexDocument> iter = naive.iterator(); iter.hasNext(); ) {
            final FlamdexDocument flamdexDocument = iter.next();
            final List<String> terms = flamdexDocument.getStringTerms(field);
            if (terms == null) {
                continue;
            }
            for (final String value : terms) {
                if (term.equals(value)) {
                    iter.remove();
                    break;
                }
            }
        }
    }

    private void checkDocuments(@Nonnull final FlamdexReader reader, @Nonnull final Set<FlamdexDocument> naive) throws IOException {
        final LongSet result = new LongOpenHashSet(naive.size());
        final LongSet expected = new LongOpenHashSet(naive.size());
        for (final FlamdexDocument document : naive) {
            final LongList original = document.getIntTerms("original");
            assertNotNull(original);
            assertEquals(1, original.size());
            expected.add(original.getLong(0));
        }
        {
            final IntTermDocIterator iterator = reader.getIntTermDocIterator("original");
            while (iterator.nextTerm()) {
                final long term = iterator.term();
                final int[] buf = new int[10];
                int numDocs;
                while ((numDocs = iterator.nextDocs(buf)) != 0) {
                    assertEquals(1, numDocs);
                    result.add(term);
                }
            }
        }
        assertEquals(expected, result);
    }

    @Test
    public void testDeletion() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(makeDocument(i));
        }
        Collections.shuffle(documents);

        final Random random = new Random(0);
        final Set<FlamdexDocument> naive = new HashSet<>();
        try (final DynamicFlamdexDocWriter flamdexDocWriter = new DynamicFlamdexDocWriter(directory)) {
            for (final FlamdexDocument document : documents) {
                addDocument(flamdexDocWriter, naive, document);
                if (random.nextInt(10) == 0) {
                    removeDocument(flamdexDocWriter, naive, "original", random.nextInt(NUM_DOCS));
                }
                if (random.nextInt(200) == 0) {
                    flamdexDocWriter.flush(random.nextBoolean());
                }
            }
        }
        final FlamdexReader reader = new DynamicFlamdexReader(directory);
        checkDocuments(reader, naive);
    }

    @Test
    public void testMultipleDeletion() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(makeDocument(i));
        }
        Collections.shuffle(documents);

        final Random random = new Random(0);
        final Set<FlamdexDocument> naive = new HashSet<>();
        try (final DynamicFlamdexDocWriter flamdexDocWriter = new DynamicFlamdexDocWriter(directory)) {
            int cnt = 0;
            for (final FlamdexDocument document : documents) {
                addDocument(flamdexDocWriter, naive, document);
                if (((++cnt) % 500) == 0) {
                    removeDocument(flamdexDocWriter, naive, "mod7mod11i", random.nextInt(7));
                }
                if (random.nextInt(200) == 0) {
                    flamdexDocWriter.flush(random.nextBoolean());
                }
            }
        }
        final FlamdexReader reader = new DynamicFlamdexReader(directory);
        checkDocuments(reader, naive);
    }
}
