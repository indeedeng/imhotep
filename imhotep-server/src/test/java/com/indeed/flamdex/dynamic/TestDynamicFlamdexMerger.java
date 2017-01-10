package com.indeed.flamdex.dynamic;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author michihiko
 */
public class TestDynamicFlamdexMerger {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int NUM_DOCS = (2 * 3 * 5 * 7 * 11) + 10;
    private Path directory;

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

    private void checkDocuments(@Nonnull final FlamdexReader reader, @Nonnull final Set<FlamdexDocument> naive) {
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
    public void testMergeAllSegments() throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        final DynamicFlamdexMerger dynamicFlamdexMerger = new DynamicFlamdexMerger(
                directory,
                new MergeStrategy() {
                    @Nonnull
                    @Override
                    Collection<? extends Collection<SegmentInfo>> splitSegmentsToMerge(@Nonnull final SortedSet<SegmentInfo> segments) {
                        return Collections.singleton(segments);
                    }
                },
                executorService
        );
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(makeDocument(i));
        }
        Collections.shuffle(documents);

        final Random random = new Random(0);
        final Set<FlamdexDocument> naive = new HashSet<>();
        try (final DynamicFlamdexDocWriter flamdexDocWriterWithoutMerger = new DynamicFlamdexDocWriter(directory)) {
            for (final FlamdexDocument document : documents) {
                addDocument(flamdexDocWriterWithoutMerger, naive, document);
                if (random.nextInt(200) == 0) {
                    flamdexDocWriterWithoutMerger.flush(random.nextBoolean());
                    dynamicFlamdexMerger.updated();
                    dynamicFlamdexMerger.join();
                    checkDocuments(new DynamicFlamdexReader(directory), naive);
                }
            }
        }
        final FlamdexReader reader = new DynamicFlamdexReader(directory);
        checkDocuments(reader, naive);
    }

    @Test
    public void testMerge() throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        try (final DynamicFlamdexMerger dynamicFlamdexMerger = new DynamicFlamdexMerger(
                directory,
                new MergeStrategy.ExponentialMergeStrategy(4, 5),
                executorService
        )) {
            final List<FlamdexDocument> documents = new ArrayList<>();
            for (int i = 0; i < NUM_DOCS; ++i) {
                documents.add(makeDocument(i));
            }
            Collections.shuffle(documents);

            final Random random = new Random(0);
            final Set<FlamdexDocument> naive = new HashSet<>();
            try (final DynamicFlamdexDocWriter flamdexDocWriterWithoutMerger = new DynamicFlamdexDocWriter(directory)) {
                for (final FlamdexDocument document : documents) {
                    addDocument(flamdexDocWriterWithoutMerger, naive, document);
                    if (random.nextInt(100) == 0) {
                        flamdexDocWriterWithoutMerger.flush(random.nextBoolean());
                        dynamicFlamdexMerger.updated();
                        dynamicFlamdexMerger.join();
                        checkDocuments(new DynamicFlamdexReader(directory), naive);
                    }
                }
            }
            final FlamdexReader reader = new DynamicFlamdexReader(directory);
            checkDocuments(reader, naive);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @Test
    public void testMergeWithDeletion() throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        try (final DynamicFlamdexMerger dynamicFlamdexMerger = new DynamicFlamdexMerger(
                directory,
                new MergeStrategy() {
                    @Nonnull
                    @Override
                    Collection<? extends Collection<SegmentInfo>> splitSegmentsToMerge(@Nonnull final SortedSet<SegmentInfo> segments) {
                        return Collections.singleton(segments);
                    }
                },
                executorService)) {
            final List<FlamdexDocument> documents = new ArrayList<>();
            for (int i = 0; i < NUM_DOCS; ++i) {
                documents.add(makeDocument(i));
            }
            Collections.shuffle(documents);

            final Random random = new Random(0);
            final Set<FlamdexDocument> naive = new HashSet<>();
            try (final DynamicFlamdexDocWriter flamdexDocWriterWithoutMerger = new DynamicFlamdexDocWriter(directory)) {
                int cnt = 0;
                for (final FlamdexDocument document : documents) {
                    addDocument(flamdexDocWriterWithoutMerger, naive, document);
                    if (((++cnt) % 500) == 0) {
                        removeDocument(flamdexDocWriterWithoutMerger, naive, "mod7mod11i", random.nextInt(7));
                    }
                    if (random.nextInt(200) == 0) {
                        flamdexDocWriterWithoutMerger.flush(random.nextBoolean());
                        dynamicFlamdexMerger.updated();
                        dynamicFlamdexMerger.join();
                        checkDocuments(new DynamicFlamdexReader(directory), naive);
                    }
                }
            }
            final FlamdexReader reader = new DynamicFlamdexReader(directory);
            checkDocuments(reader, naive);
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }
}
