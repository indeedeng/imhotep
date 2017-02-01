package com.indeed.flamdex.dynamic;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author michihiko
 */

public class TestDynamicFlamdexWriter {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int NUM_DOCS = (2 * 3 * 5 * 7 * 11) + 10;
    private Path directory;

    @Before
    public void setUp() throws IOException {
        directory = temporaryFolder.getRoot().toPath();
    }

    @Test
    public void testSimpleStats() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        final Random random = new Random(0);
        Collections.shuffle(documents, random);

        final Path indexDirectory;
        try (final DynamicFlamdexDocWriter flamdexDocWriter = new DynamicFlamdexDocWriter(directory, "shared")) {
            int commitId = 0;
            for (final FlamdexDocument document : documents) {
                if (random.nextInt(200) == 0) {
                    flamdexDocWriter.commit(commitId++, random.nextBoolean());
                }
                flamdexDocWriter.addDocument(document);
            }
            indexDirectory = flamdexDocWriter.commit(commitId++).get();
        }
        try (final FlamdexReader flamdexReader = new DynamicFlamdexReader(indexDirectory)) {
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

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testDeletion() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        final Random random = new Random(0);
        Collections.shuffle(documents, random);

        final Set<FlamdexDocument> naive = new HashSet<>();
        final Path indexDirectory;
        try (final DynamicFlamdexDocWriter flamdexDocWriter = new DynamicFlamdexDocWriter(directory, "index")) {
            int commitId = 0;
            for (final FlamdexDocument document : documents) {
                if (random.nextInt(200) == 0) {
                    flamdexDocWriter.commit(commitId++, random.nextBoolean());
                }
                if (random.nextInt(10) == 0) {
                    DynamicFlamdexTestUtils.removeDocument(naive, flamdexDocWriter, "original", random.nextInt(NUM_DOCS));
                }
                DynamicFlamdexTestUtils.addDocument(naive, flamdexDocWriter, document);
            }
            indexDirectory = flamdexDocWriter.commit(commitId++).get();
        }
        final FlamdexReader reader = new DynamicFlamdexReader(indexDirectory);
        DynamicFlamdexTestUtils.validateIndex(naive, reader);
    }

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testMultipleDeletion() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        final Random random = new Random(0);
        Collections.shuffle(documents, random);

        final Set<FlamdexDocument> naive = new HashSet<>();
        final Path indexDirectory;
        try (final DynamicFlamdexDocWriter flamdexDocWriter = new DynamicFlamdexDocWriter(directory, "index")) {
            int cnt = 0;
            for (final FlamdexDocument document : documents) {
                if (random.nextInt(200) == 0) {
                    flamdexDocWriter.commit(cnt, random.nextBoolean());
                }
                if (((++cnt) % 500) == 0) {
                    DynamicFlamdexTestUtils.removeDocument(naive, flamdexDocWriter, "mod7mod11i", random.nextInt(7));
                }
                DynamicFlamdexTestUtils.addDocument(naive, flamdexDocWriter, document);
            }
            indexDirectory = flamdexDocWriter.commit(cnt++).get();
        }
        final FlamdexReader reader = new DynamicFlamdexReader(indexDirectory);
        DynamicFlamdexTestUtils.validateIndex(naive, reader);
    }

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testWithDeletionAndMerge() throws IOException, InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        final Random random = new Random(0);
        Collections.shuffle(documents, random);

        final Set<FlamdexDocument> naive = new HashSet<>();
        final Path indexDirectory;
        try (final DynamicFlamdexDocWriter flamdexDocWriter = new DynamicFlamdexDocWriter(directory, "index", new MergeStrategy.ExponentialMergeStrategy(4), executorService)) {
            int cnt = 0;
            for (final FlamdexDocument document : documents) {
                if (random.nextInt(50) == 0) {
                    flamdexDocWriter.commit(cnt, random.nextBoolean());
                }
                if (((++cnt) % 500) == 0) {
                    DynamicFlamdexTestUtils.removeDocument(naive, flamdexDocWriter, "mod7mod11i", random.nextInt(7));
                }
                DynamicFlamdexTestUtils.addDocument(naive, flamdexDocWriter, document);
            }
            indexDirectory = flamdexDocWriter.commit(cnt).get();
        }
        final FlamdexReader reader = new DynamicFlamdexReader(indexDirectory);
        DynamicFlamdexTestUtils.validateIndex(naive, reader);
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testWriteOnExistingDynamicIndex() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        final Random random = new Random(0);
        Collections.shuffle(documents, random);

        final Set<FlamdexDocument> naive = new HashSet<>();
        final Path firstHalf;
        try (final DynamicFlamdexDocWriter flamdexDocWriter =
                     new DynamicFlamdexDocWriter(
                             directory,
                             "index",
                             new MergeStrategy.ExponentialMergeStrategy(4),
                             MoreExecutors.sameThreadExecutor())
        ) {
            int cnt = 0;
            for (final FlamdexDocument document : documents.subList(0, NUM_DOCS / 2)) {
                if (random.nextInt(50) == 0) {
                    flamdexDocWriter.commit(cnt, random.nextBoolean());
                }
                if (((++cnt) % 500) == 0) {
                    DynamicFlamdexTestUtils.removeDocument(naive, flamdexDocWriter, "mod7mod11i", random.nextInt(7));
                }
                DynamicFlamdexTestUtils.addDocument(naive, flamdexDocWriter, document);
            }
            firstHalf = flamdexDocWriter.commit(cnt).get();
        }
        final Path indexDirectory;
        try (final DynamicFlamdexDocWriter flamdexDocWriter =
                     new DynamicFlamdexDocWriter(
                             directory,
                             "index",
                             firstHalf,
                             new MergeStrategy.ExponentialMergeStrategy(2),
                             MoreExecutors.sameThreadExecutor())
        ) {
            int cnt = 0;
            for (final FlamdexDocument document : documents.subList(NUM_DOCS / 2, NUM_DOCS)) {
                if (random.nextInt(50) == 0) {
                    flamdexDocWriter.commit(cnt, random.nextBoolean());
                }
                if (((++cnt) % 500) == 0) {
                    DynamicFlamdexTestUtils.removeDocument(naive, flamdexDocWriter, "mod7mod11i", random.nextInt(7));
                }
                DynamicFlamdexTestUtils.addDocument(naive, flamdexDocWriter, document);
            }
            indexDirectory = flamdexDocWriter.commit(cnt).get();
        }
        final FlamdexReader reader = new DynamicFlamdexReader(indexDirectory);
        DynamicFlamdexTestUtils.validateIndex(naive, reader);
    }

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testWriteOnExistingStaticIndex() throws IOException {
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        final Random random = new Random(0);
        Collections.shuffle(documents, random);

        final Set<FlamdexDocument> naive = new HashSet<>();
        final Path firstHalf = temporaryFolder.newFolder("static").toPath();
        try (final FlamdexDocWriter flamdexDocWriter = new SimpleFlamdexDocWriter(firstHalf, new SimpleFlamdexDocWriter.Config())) {
            for (final FlamdexDocument document : documents.subList(0, NUM_DOCS / 2)) {
                DynamicFlamdexTestUtils.addDocument(naive, flamdexDocWriter, document);
            }
        }
        final Path indexDirectory;
        try (final DynamicFlamdexDocWriter flamdexDocWriter =
                     new DynamicFlamdexDocWriter(
                             directory,
                             "index",
                             firstHalf,
                             new MergeStrategy.ExponentialMergeStrategy(2),
                             MoreExecutors.sameThreadExecutor())
        ) {
            int cnt = 0;
            for (final FlamdexDocument document : documents.subList(NUM_DOCS / 2, NUM_DOCS)) {
                if (random.nextInt(50) == 0) {
                    flamdexDocWriter.commit(cnt, random.nextBoolean());
                }
                if (((++cnt) % 500) == 0) {
                    DynamicFlamdexTestUtils.removeDocument(naive, flamdexDocWriter, "mod7mod11i", random.nextInt(7));
                }
                DynamicFlamdexTestUtils.addDocument(naive, flamdexDocWriter, document);
            }
            indexDirectory = flamdexDocWriter.commit(cnt).get();
        }
        final FlamdexReader reader = new DynamicFlamdexReader(indexDirectory);
        DynamicFlamdexTestUtils.validateIndex(naive, reader);
    }
}
