package com.indeed.flamdex.dynamic;

import com.google.common.base.Optional;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.simple.SimpleFlamdexDocWriter;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.util.io.Directories;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author michihiko
 */

public class TestDynamicFlamdexIndexCommitter {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String INDEX_DIRECTORY_PREFIX = "testIndex";

    @Before
    public void setUp() {
    }

    @Nonnull
    private List<FlamdexDocument> generateDocuments(final int lowerBound, final int upperBound) {
        final List<FlamdexDocument> documents = new ArrayList<>(upperBound - lowerBound);
        for (int docId = lowerBound; docId < upperBound; ++docId) {
            documents.add(DynamicFlamdexTestUtils.makeDocument(docId));
        }
        return documents;
    }

    private void writeSegment(@Nullable final Set<FlamdexDocument> naive, @Nonnull final Path segmentDirectory, @Nonnull final Collection<FlamdexDocument> documents) throws IOException {
        try (final SimpleFlamdexDocWriter writer = new SimpleFlamdexDocWriter(segmentDirectory, new SimpleFlamdexDocWriter.Config())) {
            for (final FlamdexDocument document : documents) {
                writer.addDocument(document);
            }
        }
        if (naive != null) {
            naive.addAll(documents);
        }
    }

    @Test
    public void testAddSegmentsSimple() throws IOException, ExecutionException, InterruptedException {
        final Path datasetDirectory = temporaryFolder.newFolder("temp").toPath();
        long version = 0;
        try (
                final DynamicFlamdexIndexCommitter committer = new DynamicFlamdexIndexCommitter(
                        datasetDirectory,
                        INDEX_DIRECTORY_PREFIX,
                        null)
        ) {
            final Set<FlamdexDocument> naiveResult = new HashSet<>();
            final Set<Path> segmentDirectories = new HashSet<>();
            int docId = 0;
            for (int segmentId = 0; segmentId < 3; ++segmentId) {
                final Path newSegmentDirectory = committer.newSegmentDirectory();
                segmentDirectories.add(newSegmentDirectory);

                writeSegment(naiveResult, newSegmentDirectory, generateDocuments(docId, docId + 10));
                docId += 10;

                final Path indexDirectory = committer.addSegmentWithDeletionAndCommit(++version, newSegmentDirectory, null);
                assertEquals(segmentId + 1, committer.getCurrentSegmentPaths().size());

                try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(indexDirectory)) {
                    DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
                }
            }
            assertEquals(segmentDirectories, new HashSet<>(committer.getCurrentSegmentPaths()));
        }
        assertEquals("Temporary directories must be cleared", version, Directories.count(datasetDirectory));
    }

    @Test
    public void testAddSegmentsWithDeleteQuery() throws IOException, ExecutionException, InterruptedException {
        final Path datasetDirectory = temporaryFolder.newFolder("temp").toPath();
        long version = 0;
        try (
                final DynamicFlamdexIndexCommitter committer = new DynamicFlamdexIndexCommitter(
                        datasetDirectory,
                        INDEX_DIRECTORY_PREFIX,
                        null)
        ) {
            final Set<FlamdexDocument> naiveResult = new HashSet<>();
            int docId = 0;
            for (int segmentId = 0; segmentId < 3; ++segmentId) {
                final Path newSegmentDirectory = committer.newSegmentDirectory();
                naiveResult.clear();

                writeSegment(naiveResult, newSegmentDirectory, generateDocuments(docId, docId + 10));
                docId += 10;
                final Path indexDirectory = committer.addSegmentWithDeletionAndCommit(
                        ++version,
                        newSegmentDirectory,
                        Query.newRangeQuery("original", 0, 100, true) // deletes all previous documents
                );
                assertEquals(segmentId + 1, committer.getCurrentSegmentPaths().size());

                try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(indexDirectory)) {
                    DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
                }
            }
        }
    }

    @Test
    public void testGenerateSegmentNames() throws IOException, ExecutionException, InterruptedException {
        final Path datasetDirectory = temporaryFolder.newFolder("temp").toPath();
        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        try (
                final DynamicFlamdexIndexCommitter committer = new DynamicFlamdexIndexCommitter(
                        datasetDirectory,
                        INDEX_DIRECTORY_PREFIX,
                        null)
        ) {
            final List<Future<List<Path>>> futurePathLists = new ArrayList<>();
            for (int i = 0; i < 4; ++i) {
                futurePathLists.add(executorService.submit(new Callable<List<Path>>() {
                    @Override
                    public List<Path> call() throws IOException {
                        final List<Path> segmentNames = new ArrayList<>(100);
                        for (int i = 0; i < 200; ++i) {
                            segmentNames.add(committer.newSegmentDirectory());
                        }
                        return segmentNames;
                    }
                }));
            }
            final List<Path> paths = new ArrayList<>();
            for (final Future<List<Path>> futurePathList : futurePathLists) {
                paths.addAll(futurePathList.get());
            }
            // Check duplicates
            {
                final Set<Path> set = new HashSet<>(paths);
                assertEquals(paths.size(), set.size());
            }
            // Check existence
            for (final Path path : paths) {
                assertTrue(Files.isDirectory(path));
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testReplace() throws IOException {
        final Path datasetDirectory = temporaryFolder.newFolder("temp").toPath();
        long version = 0;
        try (
                final DynamicFlamdexIndexCommitter committer = new DynamicFlamdexIndexCommitter(
                        datasetDirectory,
                        INDEX_DIRECTORY_PREFIX,
                        null)
        ) {
            final Set<FlamdexDocument> naiveResult = new HashSet<>();
            final Set<Path> segmentDirectories = new HashSet<>();
            int docId = 0;
            for (int segmentId = 0; segmentId < 3; ++segmentId) {
                final Path newSegmentDirectory = committer.newSegmentDirectory();
                segmentDirectories.add(newSegmentDirectory);
                writeSegment(naiveResult, newSegmentDirectory, generateDocuments(docId, docId + 10));
                docId += 10;

                committer.addSegmentWithDeletionAndCommit(
                        ++version,
                        newSegmentDirectory,
                        Query.newRangeQuery("original", 0, 100, true) // deletes all previous documents
                );
            }
            assertEquals(segmentDirectories, new HashSet<>(committer.getCurrentSegmentPaths()));

            final Path mergedSegmentDirectory = committer.newSegmentDirectory();
            // This segment has the same documents as previous three segments.
            writeSegment(null, mergedSegmentDirectory, generateDocuments(0, docId));
            final Optional<Path> indexDirectoryOrAbsent = committer.replaceSegmentsAndCommitIfPossible(segmentDirectories, mergedSegmentDirectory);

            assertEquals(Collections.singleton(mergedSegmentDirectory), new HashSet<>(committer.getCurrentSegmentPaths()));
            assertTrue(indexDirectoryOrAbsent.isPresent());

            try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(indexDirectoryOrAbsent.get())) {
                DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
            }
        }
    }

    @Test
    public void testLazyCommit() throws IOException {
        final Path datasetDirectory = temporaryFolder.newFolder("temp").toPath();
        long version = 0;
        try (
                final DynamicFlamdexIndexCommitter committer = new DynamicFlamdexIndexCommitter(
                        datasetDirectory,
                        INDEX_DIRECTORY_PREFIX,
                        null)
        ) {
            final Set<FlamdexDocument> naiveResult = new HashSet<>();
            {
                final Path segmentDirectory = committer.newSegmentDirectory();
                writeSegment(naiveResult, segmentDirectory, generateDocuments(0, 10));
                final Optional<Path> indexDirectoryOrAbsent = committer.replaceSegmentsAndCommitIfPossible(Collections.<Path>emptyList(), segmentDirectory);
                assertFalse(indexDirectoryOrAbsent.isPresent());
            }

            final Path newSegmentDirectory = committer.newSegmentDirectory();
            writeSegment(naiveResult, newSegmentDirectory, generateDocuments(10, 20));
            final Path indexDirectory = committer.addSegmentWithDeletionAndCommit(1, newSegmentDirectory, null);

            try (final DynamicFlamdexReader reader = new DynamicFlamdexReader(indexDirectory)) {
                DynamicFlamdexTestUtils.validateIndex(naiveResult, reader);
            }
        }

    }
}
