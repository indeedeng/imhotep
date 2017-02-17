package com.indeed.flamdex.dynamic;

import com.google.common.collect.Iterables;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.writer.FlamdexDocument;
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
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;

/**
 * @author michihiko
 */

public class TestDynamicFlamdexMerger {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int NUM_DOCS = (2 * 3 * 5 * 7 * 11) + 10;
    private static final String INDEX_DIRECTORY_PREFIX = "testIndex";

    private Set<FlamdexDocument> setupedNaiveResult = null;
    private Path setupedIndexDirectory = null;

    @Before
    public void setUp() throws IOException {
        final Path datasetDirectory = temporaryFolder.newFolder("dataset").toPath();

        final Random random = new Random(0);
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        Collections.shuffle(documents, random);

        setupedNaiveResult = new HashSet<>();
        try (final DynamicFlamdexDocWriter flamdexDocWriterWithoutMerger = DynamicFlamdexDocWriter.builder()
                .setDatasetDirectory(datasetDirectory)
                .setIndexDirectoryPrefix(INDEX_DIRECTORY_PREFIX)
                .build()
        ) {
            long commitId = 0;
            for (final FlamdexDocument document : documents) {
                if (random.nextInt(100) == 0) {
                    flamdexDocWriterWithoutMerger.commit(commitId++, random.nextBoolean());
                }
                DynamicFlamdexTestUtils.addDocument(setupedNaiveResult, flamdexDocWriterWithoutMerger, document);
            }
            setupedIndexDirectory = flamdexDocWriterWithoutMerger.commit(commitId++).get();
        }
    }

    private Path mergeSegmentsWithValidation(@Nonnull final Path indexDirectory, @Nonnull final Set<FlamdexDocument> naive, @Nonnull final MergeStrategy mergeStrategy, @Nonnull final ExecutorService executorService) throws IOException, ExecutionException, InterruptedException {
        final Path outputDirectory = temporaryFolder.newFolder("output").toPath();

        try (final IndexCommitterWithValidate indexCommitter = new IndexCommitterWithValidate(naive, outputDirectory, INDEX_DIRECTORY_PREFIX, indexDirectory)) {
            try (final DynamicFlamdexMerger dynamicFlamdexMerger = new DynamicFlamdexMerger(indexCommitter, mergeStrategy, executorService)) {
                dynamicFlamdexMerger.updated();
                dynamicFlamdexMerger.join();
            }
            return Iterables.getLast(indexCommitter.getIndexDirectories());
        }
    }

    @Test
    public void testMergeAllSegments() throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executorService = newFixedThreadPool(4);
        final Path indexDirectory = mergeSegmentsWithValidation(
                setupedIndexDirectory,
                setupedNaiveResult,
                new MergeStrategy() {
                    @Nonnull
                    @Override
                    public Collection<? extends Collection<Segment>> splitSegmentsToMerge(@Nonnull final List<Segment> availableSegments) {
                        return Collections.singleton(availableSegments);
                    }
                },
                executorService
        );
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
        assertEquals(1, DynamicFlamdexIndexUtil.listSegmentDirectories(indexDirectory).size());
    }

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testExponentialMerge() throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executorService = newFixedThreadPool(4);
        mergeSegmentsWithValidation(
                setupedIndexDirectory,
                setupedNaiveResult,
                new MergeStrategy.ExponentialMergeStrategy(4, 5),
                executorService
        );
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testWriterWithMerge() throws IOException, InterruptedException {
        final MergeStrategy mergeStrategy = new MergeStrategy.ExponentialMergeStrategy(4, 5);
        final ExecutorService executorService = Executors.newFixedThreadPool(4);

        final Path datasetDirectory = temporaryFolder.newFolder("dataset").toPath();
        final Random random = new Random(0);
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        Collections.shuffle(documents, random);

        final Path indexDirectory;
        final Set<FlamdexDocument> naiveResult = new HashSet<>();
        try (final DynamicFlamdexDocWriter flamdexDocWriterWithMerger = DynamicFlamdexDocWriter.builder()
                .setDatasetDirectory(datasetDirectory)
                .setIndexDirectoryPrefix(INDEX_DIRECTORY_PREFIX)
                .setMergeStrategy(mergeStrategy)
                .setExecutorService(executorService)
                .build()) {
            long commitId = 0;
            for (final FlamdexDocument document : documents) {
                if (random.nextInt(100) == 0) {
                    flamdexDocWriterWithMerger.commit(commitId++, random.nextBoolean());
                }
                DynamicFlamdexTestUtils.addDocument(naiveResult, flamdexDocWriterWithMerger, document);
            }
            indexDirectory = flamdexDocWriterWithMerger.commit(commitId++).get();
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        try (final FlamdexReader reader = new DynamicFlamdexReader(indexDirectory)) {
            DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
        }
    }

    @SuppressWarnings("JUnitTestMethodWithNoAssertions")
    @Test
    public void testMergeWithDeletion() throws IOException, ExecutionException, InterruptedException {
        final MergeStrategy mergeStrategy = new MergeStrategy.ExponentialMergeStrategy(4, 5);
        final ExecutorService executorService = Executors.newFixedThreadPool(4);

        final Path datasetDirectory = temporaryFolder.newFolder("dataset").toPath();
        final Random random = new Random(0);
        final List<FlamdexDocument> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; ++i) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(i));
        }
        Collections.shuffle(documents, random);

        final Path indexDirectory;
        final Set<FlamdexDocument> naiveResult = new HashSet<>();
        try (final DynamicFlamdexDocWriter flamdexDocWriterWithoutMerger = DynamicFlamdexDocWriter.builder()
                .setDatasetDirectory(datasetDirectory)
                .setIndexDirectoryPrefix(INDEX_DIRECTORY_PREFIX)
                .setMergeStrategy(mergeStrategy)
                .setExecutorService(executorService)
                .build()
        ) {
            long commitId = 0;
            for (final FlamdexDocument document : documents) {
                if (random.nextInt(100) == 0) {
                    flamdexDocWriterWithoutMerger.commit(commitId++, random.nextBoolean());
                }
                DynamicFlamdexTestUtils.addDocument(naiveResult, flamdexDocWriterWithoutMerger, document);
                if (random.nextInt(500) == 0) {
                    DynamicFlamdexTestUtils.removeDocument(naiveResult, flamdexDocWriterWithoutMerger, "mod7mod11i", random.nextInt(7));
                }
            }
            indexDirectory = flamdexDocWriterWithoutMerger.commit(commitId++).get();
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
        try (final FlamdexReader reader = new DynamicFlamdexReader(indexDirectory)) {
            DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
        }
    }
}
