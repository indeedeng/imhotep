package com.indeed.imhotep.fs;

import com.google.common.base.Throwables;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.AccessControlException;
import java.util.LinkedList;

/**
 * @author darren
 */
class LruCache {
    private static final Logger LOGGER = Logger.getLogger(LruCache.class);
    private static final String FILE_SUFFIX = ".cached_file";
    private static final String DIRECTORY_PREFIX = "cache_directory_";

    private final TObjectIntHashMap<RemoteCachingPath> openFiles = new TObjectIntHashMap<>(128, 0.75f, 0);
    private final LinkedList<RemoteCachingPath> lruList = new LinkedList<>();
    private long spaceRemaining;
    private final Path cacheDirectory;
    private final int defaultReservation;
    private final FileTracker localOnlyFileTracker;
    private final Loader loader;

    LruCache(final Path cacheDirectory,
             final int defaultReservation,
             final long cacheSize,
             final FileTracker fileTracker,
             final Loader loader) {
        spaceRemaining = cacheSize;
        this.loader = loader;
        this.cacheDirectory = cacheDirectory;
        this.defaultReservation = defaultReservation;
        localOnlyFileTracker = fileTracker;
    }

    Path getAndOpenForRead(final RemoteCachingPath path) throws IOException {
        final Path cachePath = getCachePath(path);
        if (Files.notExists(cachePath)) {
            final long fileSize = loader.getFileSize(path);
            if (fileSize == -1) {
                throw new FileNotFoundException(path + " does not exist");
            }
            loadFile(path, cachePath, fileSize);
        } else {
            markFileOpen(path);
        }
        return cachePath;
    }

    FileSizeWatcher getAndOpenForWrite(final RemoteCachingPath path) throws IOException {
        final Path cachePath = getCachePath(path);

        // TODO: this is some seriously buggy code
        // Check if is present in the cache
        // -> fail if not present
        // Check if local only
        // -> fail otherwise (read only)

        if (Files.notExists(cachePath)) {
            /* file not present remotely */
            throw new FileNotFoundException("File does not exist: " + path);
        }

        if (!localOnlyFileTracker.contains(path)) {
            /* Must be a remote file */
            throw new AccessControlException("Remote files are read only. Path " + path);
        }

        markFileOpen(path);
        return new FileSizeWatcher(Files.size(cachePath), 0, cachePath);
    }

    /**
     *
     * @param path
     * @param failOnExists
     * @return
     * @throws IOException
     */
    FileSizeWatcher addNewFile(final RemoteCachingPath path, final boolean failOnExists) throws
            IOException {
        final Path cachePath = getCachePath(path);

        if (Files.exists(path)) {
            // if it already exists, it can either be
            if (failOnExists) {
                throw new FileAlreadyExistsException("File or directory already exist for " + path);
            } else if (Files.isDirectory(path)) {
                // 1. a local or remote directory
                throw new FileAlreadyExistsException("Directory already exist for " + path);
            } else if (!localOnlyFileTracker.contains(path)) {
                // 2. a remote file
                throw new AccessControlException("Cannot create non-local file at path " + path);
            } else {
                // 3. a local file
                path.setLocalOnly(true);
                markFileOpen(path);
                return new FileSizeWatcher(Files.size(cachePath), 0, cachePath);
            }
        } else {
            Files.createFile(cachePath);
            path.setLocalOnly(true);
            trackFile(path);
            return new FileSizeWatcher(0, defaultReservation, cachePath);
        }
    }

    synchronized void markFileOpen(RemoteCachingPath path) {
        final int openCount = openFiles.get(path) + 1;
        openFiles.put(path, openCount);
    }

    synchronized void markFileClosed(RemoteCachingPath path) {
        final int openCount = openFiles.get(path) - 1;
        if (openCount == 0) {
            openFiles.remove(path);
            lruList.addFirst(path);
        } else {
            openFiles.put(path, openCount);
        }
    }

    private Path loadFile(final RemoteCachingPath path, final Path cachePath, final long fileSize)
            throws IOException {
        reserveSpace(fileSize);
        try {
            final Path tmpPath = generateTmpPath();
            loader.load(path, tmpPath);

            Files.createDirectories(cachePath.getParent());
            tryMove(tmpPath, cachePath);
        } catch (final Throwable e) {
            releaseSpace(fileSize);
            throw Throwables.propagate(e);
        }

        path.setLocalOnly(false);
        trackFile(path);
        return cachePath;
    }

    private void tryMove(final Path tmpPath, final Path cachePath) throws IOException {
        Files.move(tmpPath,
                cachePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    private Path generateTmpPath() throws IOException {
        return Files.createTempFile(cacheDirectory, "cache", null);
    }

    private Path getCachePath(final RemoteCachingPath remotePath) {
        /* find a directory with room */
        final String hash = DigestUtils.md5Hex(remotePath.normalize().toString());

        Path directory = cacheDirectory.resolve(DIRECTORY_PREFIX + hash.substring(0, 2));
        directory = directory.resolve(DIRECTORY_PREFIX + hash.substring(2, 4));
        return directory.resolve(hash.substring(4) + FILE_SUFFIX);
    }

    private synchronized void releaseSpace(final long amt) {
        spaceRemaining += amt;
    }

    /**
     * try to ensure that there is enough space for the specified file size
     *
     * @param fileSize the file size to ensure
     * @throws IOException
     */
    private synchronized void reserveSpace(final long fileSize) throws IOException {
        if (spaceRemaining >= fileSize) {
            spaceRemaining -= fileSize;
            return;
        }

        final long evicted = evictFiles(fileSize - spaceRemaining);
        spaceRemaining -= evicted;
        if (spaceRemaining >= fileSize) {
            spaceRemaining -= fileSize;
        } else {
            throw new IOException("Not enough free space available");
        }
    }

    private long evictFiles(final long numBytes) {
        long bytesEvicted = 0;

        while ((bytesEvicted < numBytes) && !lruList.isEmpty()) {
            final RemoteCachingPath victimPath = lruList.removeLast();
            try {
                if (openFiles.get(victimPath) == 0) {
                    final Path cachePath = getCachePath(victimPath);
                    if (Files.exists(cachePath)) {
                        final long size = Files.size(cachePath);
                        evictFile(victimPath);
                        bytesEvicted += size;
                    }
                }
            } catch (final IOException e) {
                LOGGER.warn("Failed to evict cache file for " + victimPath, e);
            }
        }

        return bytesEvicted;
    }

    private void evictFile(final RemoteCachingPath path) throws IOException {
        Files.delete(getCachePath(path));
        if (path.isLocalOnly()) {
            localOnlyFileTracker.removeFile(path);
        }
    }

    private synchronized void trackFile(RemoteCachingPath remotePath) throws IOException {
        openFiles.put(remotePath, 1);
        if (remotePath.isLocalOnly()) {
            localOnlyFileTracker.addFile(remotePath, getCachePath(remotePath));
        }
    }

    synchronized boolean isPresent(RemoteCachingPath path) {
        final Path cachePath = getCachePath(path);
        if (cachePath != null && Files.exists(cachePath)) {
            return true;
        }
        return false;
    }

    class FileSizeWatcher {
        private long size;
        private int reservation;
        private final Path path;

        FileSizeWatcher(long size, int initialReservation, Path path) throws IOException {
            this.size = size;
            this.reservation = initialReservation;
            this.path = path;

            if (initialReservation > 0) {
                reserveSpace(initialReservation);
            }
        }

        Path getPath() {
            return path;
        }

        void trackAppend(final long amount) throws IOException {
            reservation -= amount;
            if (reservation < 0) {
                reserveSpace(-reservation);
                reservation = 0;
            }
            this.size += amount;
        }

        void trackClose() throws IOException {
            final long finalSize = Files.size(path);
            if (finalSize > size) {
                reserveSpace(finalSize - size);
            } else if (finalSize < size) {
                releaseSpace(size - finalSize);
            }
        }
    }

    interface Loader {
        void load(RemoteCachingPath path, Path tmpPath) throws IOException;

        /**
         * Get the file size of the corresponding resource
         *
         * @param path
         * @return the file size. -1 if it does not exist
         * @throws IOException
         */
        long getFileSize(RemoteCachingPath path) throws IOException;
    }
}
