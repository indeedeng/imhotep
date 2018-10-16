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
package com.indeed.imhotep.fs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;
import com.indeed.imhotep.fs.sql.SqarMetaDataDao;
import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.serialization.BooleanSerializer;
import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.StringSerializer;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 *  LSM tree backed cache of SQAR metadata
 *  For efficiency of both directory listing and single file detailed metadata operations
 *  we store the complete shard files listing as a separate LSM tree entry (key ending with '~')
 *  and complete metadata for each file in separate LSM tree entries.
 *  This way both getFileMetadata() and listDirectory() operations require only 1 LSM tree lookup.
 *
 * key: (shardPath|filePath)
 * value: (Value objects containing either a list of short file listings or a single detailed file entry)
 */
class SqarMetaDataLSMStore implements SqarMetaDataDao, Closeable {
    private static final Logger log = Logger.getLogger(SqarMetaDataLSMStore.class);

    private static final IntSerializer        intSerializer     = new IntSerializer();
    private static final BooleanSerializer    booleanSerializer = new BooleanSerializer();
    private static final LongSerializer       longSerializer    = new LongSerializer();
    private static final SqarFileEntrySerializer sqarFileEntrySerializer = new SqarFileEntrySerializer();
    private static final SqarFileListingSerializer sqarFileListingSerializer = new SqarFileListingSerializer();
    private static final StringSerializer     strSerializer     = new StringSerializer();
    private static final ValueSerializer      valueSerializer   = new ValueSerializer();

    private static final String PATH_DELIMITER = "/";
    private static final String SHARD_PATH_AND_FILE_PATH_DELIMITER = "|";
    // used to distinguish between the listing entry and the single root directory entry
    private static final String LISTING_KEY_SUFFIX = "~";

    private final Store<String, Value> store;
    private final Duration expirationDuration;
    private final Timer trimTimer;

    SqarMetaDataLSMStore(final File root, @Nullable final Duration expirationDuration) throws IOException {
        this.expirationDuration = expirationDuration;
        final StoreBuilder<String, Value> builder =
            new StoreBuilder<>(root, strSerializer, valueSerializer);
        builder.setCodec(new SnappyCodec());
        builder.setStorageType(StorageType.BLOCK_COMPRESSED);
        store = builder.build();

        final TimerTask trimTask = new TimerTask() {
            @Override
            public void run() {
                trim();
            }
        };
        if(expirationDuration != null) {
            this.trimTimer = new Timer("SqarMetaDataLSMStoreTrimmer", true);
            long trimIntervalMillis = TimeUnit.DAYS.toMillis(1);
            this.trimTimer.schedule(trimTask, trimIntervalMillis, trimIntervalMillis);
        } else {
            this.trimTimer = null;
        }
    }

    @Override public void close() throws IOException {
        store.close();
        try {
            store.waitForCompactions();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for the SQAR metadata cache LSM tree to close", e);
        }
        if(trimTimer != null) {
            trimTimer.cancel();
        }
    }

    Iterator<Store.Entry<String, Value>> iterator() throws IOException  {
        return store.iterator();
    }

    boolean containsKey(final String key) throws IOException { return store.containsKey(key); }

    void put(final String key, final Value value) throws IOException { store.put(key, value); }
    Value get(final String key) throws IOException { return store.get(key); }

    void delete(final String key) throws IOException { store.delete(key); }

    void sync() throws IOException { store.sync(); }

    static boolean lsmTreeExistsInDir(final File dirToCheck) throws IOException {
        if (dirToCheck == null) {
            return false;
        }

        final Path lsmTreePath = dirToCheck.toPath();

        if (Files.notExists(lsmTreePath)) {
            return false;
        }
        if (!Files.isDirectory(lsmTreePath)) {
            return false;
        }

        boolean foundLatest = false;
        boolean foundData = false;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(lsmTreePath)) {
            for (final Path child : dirStream) {
                if (child.getFileName().toString().equals("latest")) {
                    foundLatest = true;
                }
                if (child.getFileName().toString().equals("data")) {
                    foundData = true;
                }
            }
        }

        return foundLatest && foundData;
    }

    /** Attempt to safely delete a ShardStore. Since the failure mode for
     * deleting the wrong directory can be extreme, this code tries to
     * heuristically confirm that storeDir contains an LSM tree by looking for
     * telltale files within it.
     */
    static void deleteExisting(final File shardStoreDir) throws IOException {
        if(lsmTreeExistsInDir(shardStoreDir)) {
            PosixFileOperations.rmrf(shardStoreDir);
        }
    }

    @Override
    public void cacheMetadata(Path shardPath, Iterable<RemoteFileMetadata> metadataList) {
        final int cachingTimestamp = (int)(System.currentTimeMillis() / 1000); // seconds since epoch
        final List<SqarFileEntry> sqarFilesEntries = new ArrayList<>();
        final List<SqarFileListing> sqarFilesListing = new ArrayList<>();
        final Path normalizedShardPath = shardPath.normalize();
        final String key = normalizedShardPath.toString();
        for (final RemoteFileMetadata remoteFileMetadata : metadataList) {
            final FileMetadata fileMetadata = remoteFileMetadata.getFileMetadata();
            final String filePath = remoteFileMetadata.isFile() ? fileMetadata.getFilename() : toNormalizedDirName(fileMetadata.getFilename());
            sqarFilesListing.add(new SqarFileListing(filePath, remoteFileMetadata.isFile(), fileMetadata.getSize()));

            final SqarFileEntry fileEntry = new SqarFileEntry(
                    filePath,
                    remoteFileMetadata.isFile(),
                    fileMetadata.getSize(),
                    fileMetadata.getTimestamp(),
                    fileMetadata.getChecksum(),
                    fileMetadata.getStartOffset(),
                    fileMetadata.getCompressor().getKey(),
                    fileMetadata.getArchiveFilename(),
                    remoteFileMetadata.getCompressedSize());

            sqarFilesEntries.add(fileEntry);
        }
        // have to write the listing first so that if we fail in the middle everything will be retried on next attempt
        // since we determine whether a shard's metadata is cached using getFileMetadata() rather than listDirectory()
        final Value listingValue = new Value(cachingTimestamp, sqarFilesListing);
        final String listingKey = key + LISTING_KEY_SUFFIX;
        try {
            this.put(listingKey , listingValue);
        } catch (IOException e) {
            Throwables.propagate(e);
        }

        // now write the per file full metadata entries
        for (final SqarFileEntry sqarFileEntry : sqarFilesEntries) {
            final Value fileEntryValue = new Value(cachingTimestamp, sqarFileEntry);
            final String fileEntryKey = normalizedShardPath.toString() + SHARD_PATH_AND_FILE_PATH_DELIMITER + sqarFileEntry.filePath;
            try {
                this.put(fileEntryKey, fileEntryValue);
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
    }

    private static String toNormalizedDirName(final String dirName) {
        if ((dirName.length() > PATH_DELIMITER.length()) && dirName.endsWith(PATH_DELIMITER)) {
            return dirName.substring(0, dirName.length() - PATH_DELIMITER.length());
        }
        return dirName;
    }

    private static String toNormalizedDirNameWithSep(final String dirName) {
        if (dirName.isEmpty() || dirName.endsWith(PATH_DELIMITER)) {
            return dirName;
        }
        return dirName + PATH_DELIMITER;
    }

    @Override
    public Iterable<RemoteFileListing> listDirectory(Path shardPath, String dirname) {
        final String normalizedDirNameWithSep = toNormalizedDirNameWithSep(dirname);
        final String key = shardPath.normalize().toString() + LISTING_KEY_SUFFIX;
        try {
            final Value value = store.get(key);
            if (value == null) {
                return Collections.emptyList();
            }
            final List<RemoteFileListing> result = new ArrayList<>();
            for(final SqarFileListing sqarFileListing: value.dirListing) {
                if(sqarFileListing.filePath.startsWith(normalizedDirNameWithSep) &&
                        !(sqarFileListing.filePath.length() > normalizedDirNameWithSep.length() &&
                                sqarFileListing.filePath.substring(normalizedDirNameWithSep.length() + 1).contains(PATH_DELIMITER)) &&
                        (!normalizedDirNameWithSep.isEmpty() || !sqarFileListing.filePath.isEmpty() )) {
                    result.add(toRemoteFileListing(sqarFileListing));
                }
            }
            return result;
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Nullable
    @Override
    public RemoteFileMetadata getFileMetadata(Path shardPath, String filename) {
        final String key = shardPath.normalize().toString() + SHARD_PATH_AND_FILE_PATH_DELIMITER + filename;
        try {
            final Value value = store.get(key);
            if (value == null || value.isListing) {
                return null;
            }
            return toRemoteFileMetadata(value.sqarFileEntry);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static RemoteFileListing toRemoteFileListing(final SqarFileListing fetchedRecord) {
        if (fetchedRecord.isFile) {
            return new RemoteFileListing(fetchedRecord.filePath, true, fetchedRecord.size);
        } else {
            return new RemoteFileListing(
                    FilenameUtils.normalizeNoEndSeparator(fetchedRecord.filePath)
            );
        }
    }

    private static RemoteFileMetadata toRemoteFileMetadata(final SqarFileEntry fetchedRecord) {
        if (fetchedRecord.isFile) {
            final FileMetadata fileMetadata = new FileMetadata(
                    fetchedRecord.filePath,
                    fetchedRecord.size,
                    fetchedRecord.timestamp,
                    fetchedRecord.checksum,
                    fetchedRecord.archiveOffset,
                    SquallArchiveCompressor.fromKey(fetchedRecord.compressorType),
                    fetchedRecord.archiveName
            );
            return new RemoteFileMetadata(fileMetadata, fetchedRecord.packedSize);
        } else {
            return new RemoteFileMetadata(
                    FilenameUtils.normalizeNoEndSeparator(fetchedRecord.filePath)
            );
        }
    }

    @Override
    public boolean hasShard(Path shardPath) {
        final String key = shardPath.normalize().toString() + LISTING_KEY_SUFFIX;
        try {
            return store.containsKey(key);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Deletes old entries from the cache
     */
    @VisibleForTesting
    void trim() {
        try {
            long deletedCount = 0;
            final int oldestTimestampToKeep = (int) (System.currentTimeMillis() - expirationDuration.toMillis());
            final Iterator<Store.Entry<String, Value>> storeIterator = iterator();
            while(storeIterator.hasNext()) {
                Store.Entry<String, Value> entry = storeIterator.next();
                if(entry.getValue().cachingTimestamp < oldestTimestampToKeep) {
                    // TODO: verify that deletion during iteration is OK
                    delete(entry.getKey());
                    deletedCount++;
                }
            }
            log.info("Deleted " + deletedCount + " expired entries from the SQAR metadata cache");
        } catch (Exception e) {
            log.warn("Failed to clear old entries from the LSM tree metadata cache", e);
        }
    }

    static final class SqarFileListing {
        final String filePath;
        final boolean isFile;
        final long size;

        SqarFileListing(String filePath, boolean isFile, long size) {
            this.filePath = filePath;
            this.isFile = isFile;
            this.size = size;
        }
    }

    static final class SqarFileEntry {
        final String filePath;
        final long size;
        final boolean isFile;
        final long timestamp;
        final String checksum;
        final long archiveOffset;
        final String compressorType;
        final String archiveName;
        final long packedSize;

        /** Constructor for a directory entry */
        SqarFileEntry(String dirPath) {
            this(dirPath, false, -1, 0, "", 0, "", "", 0);
        }

        /** Constructor for a file entry */
        SqarFileEntry(String filePath, boolean isFile, long size, long timestamp, String checksum, long archiveOffset, String compressorType, String archiveName, long packedSize) {
            this.filePath = filePath;
            this.size = size;
            this.timestamp = timestamp;
            this.checksum = checksum;
            this.archiveOffset = archiveOffset;
            this.compressorType = compressorType;
            this.archiveName = archiveName;
            this.isFile = isFile;
            this.packedSize = packedSize;
        }
    }

    /**
     * Contains either a list of short file listings or a single detailed file entry
     */
    private static final class Value {
        private final int cachingTimestamp; // seconds since epoch
        private final boolean isListing;
        private final SqarFileEntry sqarFileEntry;  // only if isListing == false
        private final List<SqarFileListing> dirListing;    // only if isListing == true

        Value(int cachingTimestamp, List<SqarFileListing> files) {
            this.cachingTimestamp = cachingTimestamp;
            this.dirListing = files;
            this.sqarFileEntry = null;
            this.isListing = true;
        }

        Value(int cachingTimestamp, SqarFileEntry sqarFileEntry) {
            this.cachingTimestamp = cachingTimestamp;
            this.sqarFileEntry = sqarFileEntry;
            this.dirListing = null;
            this.isListing = false;
        }
    }

    private static final class SqarFileListingSerializer implements Serializer<SqarFileListing> {
        public void write(final SqarFileListing value, final DataOutput out) throws IOException {
            strSerializer.write(Strings.nullToEmpty(value.filePath), out);
            booleanSerializer.write(value.isFile, out);
            if (value.isFile) {
                longSerializer.write(value.size, out);
            }
        }

        public SqarFileListing read(final DataInput in) throws IOException {
            final String filePath = strSerializer.read(in);
            final boolean isFile = booleanSerializer.read(in);
            final long size;
            if (isFile) {
                size = longSerializer.read(in);
            } else {
                size = -1;
            }
            return new SqarFileListing(filePath, isFile, size);
        }
    }

    private static final class SqarFileEntrySerializer implements Serializer<SqarFileEntry> {
        public void write(final SqarFileEntry value, final DataOutput out) throws IOException {
            strSerializer.write(Strings.nullToEmpty(value.filePath), out);
            booleanSerializer.write(value.isFile, out);
            if (value.isFile) {
                longSerializer.write(value.size, out);
                longSerializer.write(value.timestamp, out);
                strSerializer.write(Strings.nullToEmpty(value.checksum), out); // TODO: is nullToEmpty ok?
                longSerializer.write(value.archiveOffset, out);
                strSerializer.write(Strings.nullToEmpty(value.compressorType), out);
                strSerializer.write(Strings.nullToEmpty(value.archiveName), out);
                longSerializer.write(value.packedSize, out);
            }
        }

        public SqarFileEntry read(final DataInput in) throws IOException {
            final String filePath = strSerializer.read(in);
            final boolean isFile = booleanSerializer.read(in);
            if (!isFile) {
                return new SqarFileEntry(filePath);
            }
            final long size = longSerializer.read(in);
            final long timestamp = longSerializer.read(in);
            final String checksum = strSerializer.read(in);
            final long archiveOffset = longSerializer.read(in);
            final String compressorType = strSerializer.read(in);
            final String archiveName = strSerializer.read(in);
            final long packedFile = longSerializer.read(in);

            return new SqarFileEntry(filePath, isFile, size, timestamp, checksum, archiveOffset, compressorType, archiveName, packedFile);
        }
    }

    private static final class ValueSerializer implements Serializer<Value> {

        public void write(final Value value, final DataOutput out)
            throws IOException {
            intSerializer.write(value.cachingTimestamp, out);
            booleanSerializer.write(value.isListing, out);
            if (value.isListing) {
                intSerializer.write(value.dirListing.size(), out);
                for (final SqarFileListing fileListing: value.dirListing) {
                    sqarFileListingSerializer.write(fileListing, out);
                }
            } else {
                sqarFileEntrySerializer.write(value.sqarFileEntry, out);
            }
        }

        public Value read(final DataInput in)
            throws IOException {
            final int cachingTimestamp = intSerializer.read(in);
            final boolean isListing = booleanSerializer.read(in);
            if (isListing) {
                final int numValues = intSerializer.read(in);
                final List<SqarFileListing> result = new ObjectArrayList<>(numValues);
                for (int count = 0; count < numValues; ++count) {
                    final SqarFileListing fileListing = sqarFileListingSerializer.read(in);
                    result.add(fileListing);
                }
                return new Value(cachingTimestamp, result);
            } else {
                final SqarFileEntry fileEntry = sqarFileEntrySerializer.read(in);
                return new Value(cachingTimestamp, fileEntry);
            }
        }
    }
}
