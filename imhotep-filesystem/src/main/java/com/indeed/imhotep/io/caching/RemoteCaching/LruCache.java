package com.indeed.imhotep.io.caching.RemoteCaching;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.AccessControlException;
import java.util.LinkedList;

/**
 * Created by darren on 11/24/15.
 */
public class LruCache {
    private static final String FILE_SUFFIX = ".cached_file";
    private static final String DIRECTORY_PREFIX = "cache_directory_";

    private final TObjectIntHashMap<RemoteCachingPath> openFiles = new TObjectIntHashMap<>(128, 0.75f, 0);
    private final LinkedList<RemoteCachingPath> lruList = new LinkedList<>();
    private long spaceRemaining;
    private final Path cacheDirectory;
    private final int defaultReservation;
    private final FileTracker localOnlyFileTracker;
    private final Loader loader;

    public LruCache(Path cacheDirectory,
                    int defaultReservation,
                    FileTracker fileTracker,
                    Loader loader) {
        this.loader = loader;
        this.cacheDirectory = cacheDirectory;
        this.defaultReservation = defaultReservation;
        this.localOnlyFileTracker = fileTracker;
    }

    public Path getAndOpenForRead(RemoteCachingPath path) throws IOException {
        final Path cachePath = getCachePath(cacheDirectory, path);
        if (Files.notExists(cachePath)) {
            final long fileSize = loader.getFileLen(path);
            if (fileSize == -1) {
                /* file not present remotely */
                return null;
            }
            loadFile(path, cachePath, fileSize);
        } else {
            markFileOpen(path);
        }
        return cachePath;
    }

    public FileSizeWatcher getAndOpenForWrite(RemoteCachingPath path) throws IOException {
        final Path cachePath = getCachePath(cacheDirectory, path);

        // Check if is present in the cache
        // -> fail if not present
        // Check if local only
        // -> fail otherwise (read only)

        if (Files.notExists(cachePath)) {
            /* file not present remotely */
            throw new FileNotFoundException("File does not exist: " + path.toString());
        }

        if (!localOnlyFileTracker.contains(path)) {
            /* Must be a remote file */
            throw new AccessControlException("Remote files are read only. Path " + path.toString());
        }

        markFileOpen(path);
        return new FileSizeWatcher(Files.size(cachePath), 0, cachePath);
    }

    public FileSizeWatcher addNewFile(RemoteCachingPath path, boolean failOnExists) throws
            IOException {
        final Path cachePath = getCachePath(cacheDirectory, path);
        if (Files.exists(cachePath)) {
            if (failOnExists) {
                throw new FileAlreadyExistsException("File " + path.toString() + " already exists");
            } else {
                markFileOpen(path);
                return new FileSizeWatcher(Files.size(cachePath), 0, cachePath);
            }
        } else if (localOnlyFileTracker.contains(path)) {
            /* a directory */
            throw new FileAlreadyExistsException("Directory exists with path " + path.toString());
        }
        Files.createFile(cachePath);
        path.setLocalOnly(true);
        trackFile(path);
        return new FileSizeWatcher(0, defaultReservation, cachePath);
    }

    synchronized void markFileOpen(RemoteCachingPath path) {
        final int openCount = openFiles.get(path) + 1;
        openFiles.put(path, openCount);
    }

    synchronized void markFileClosed(RemoteCachingPath path) {
        final int openCount = openFiles.get(path) - 1;
        if (openCount == 0) {
            openFiles.put(path, openCount);
            lruList.addFirst(path);
        }
    }

    private Path loadFile(RemoteCachingPath path, Path cachePath, long fileSize) throws
            IOException {
        reserveSpace(fileSize);
        try {
            final Path tmpPath = generateTmpPath();
            loader.load(path, tmpPath);
            if (! tryMove(tmpPath, cachePath)) {
                /* file has already been loaded */
                Files.delete(tmpPath);
                releaseSpace(fileSize);
                return cachePath;
            }
        } catch(Exception e) {
            releaseSpace(fileSize);
            throw e;
        }

        path.setLocalOnly(false);
        trackFile(path);
        return cachePath;
    }

    private synchronized boolean tryMove(Path tmpPath, Path cachePath) throws IOException {
        if (Files.exists(cachePath)) {
            return false;
        }
        Files.move(tmpPath, cachePath, StandardCopyOption.ATOMIC_MOVE);
        return true;
    }

    private Path generateTmpPath() throws IOException {
        return Files.createTempFile(cacheDirectory, null, null);
    }

    private Path getCachePath(Path topDirectory, RemoteCachingPath remotePath) {
        /* find a directory with room */
        final String hash = DigestUtils.md5Hex(remotePath.normalize().toString());

        Path directory = topDirectory.resolve(DIRECTORY_PREFIX + hash.substring(0,2));
        directory = directory.resolve(DIRECTORY_PREFIX + hash.substring(2, 4));
        return directory.resolve(hash.substring(4) + FILE_SUFFIX);
    }

    private synchronized void releaseSpace(long amt) {
        spaceRemaining += amt;
    }

    private synchronized void reserveSpace(long fileSize) throws IOException {
        if (spaceRemaining >= fileSize) {
            spaceRemaining -= fileSize;
            return;
        }

        /* delete one or more files */
        final long evicted = evictFiles(fileSize - spaceRemaining);
        spaceRemaining -= evicted;
        if (spaceRemaining >= fileSize) {
            spaceRemaining -= fileSize;
            return;
        } else {
            throw new IOException("Not enough free space available");
        }
    }

    private long evictFiles(long numBytes) {
        long bytesEvicted = 0;

        while (bytesEvicted < numBytes && !lruList.isEmpty()) {
            try {
                final RemoteCachingPath victimPath = lruList.removeLast();
                openFiles.remove(victimPath);

                final long size = Files.size(getCachePath(cacheDirectory, victimPath));
                evictFile(victimPath);
                bytesEvicted += size;
            } catch (IOException e) {
                /* just ignore */
                e.printStackTrace();
            }
        }

        return bytesEvicted;
    }

    private void evictFile(RemoteCachingPath victimPath) throws IOException {
        /* delete the file first, in case there are any problems */
        Files.delete(getCachePath(cacheDirectory, victimPath));

        if (victimPath.isLocalOnly()) {
            localOnlyFileTracker.removeFile(victimPath);
        }
    }

    private synchronized void trackFile(RemoteCachingPath remotePath) throws IOException {
        openFiles.put(remotePath, 1);
        if (remotePath.isLocalOnly()) {
            localOnlyFileTracker.addFile(remotePath, getCachePath(cacheDirectory, remotePath));
        }
    }

    public synchronized boolean isPresent(RemoteCachingPath path) {
        final Path cachePath = getCachePath(cacheDirectory, path);
        if (cachePath != null && Files.exists(cachePath))
            return true;
        return false;
    }

    class FileSizeWatcher {
        private long size;
        private int reservation;
        private final Path path;

        public FileSizeWatcher(long size, int initialReservation, Path path) throws IOException {
            this.size = size;
            this.reservation = initialReservation;
            this.path = path;

            if (initialReservation > 0) {
                reserveSpace(initialReservation);
            }
        }

        public Path getPath() {
            return path;
        }

        public void trackAppend(long amt) throws IOException {
            reservation -= amt;
            if (reservation < 0) {
                reserveSpace(-reservation);
                reservation = 0;
            }
            this.size += amt;
        }

        public void trackClose() throws IOException {
            final long finalSize = Files.size(path);
            if (finalSize > size) {
                reserveSpace(finalSize - size);
            } else if (finalSize < size){
                releaseSpace(size - finalSize);
            }
        }
    }

    interface Loader {
        void load(RemoteCachingPath path, Path tmpPath) throws IOException;

        long getFileLen(RemoteCachingPath path) throws IOException;
    }
}
