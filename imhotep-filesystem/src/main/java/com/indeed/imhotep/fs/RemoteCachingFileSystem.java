package com.indeed.imhotep.fs;

import com.google.common.collect.ImmutableSet;
import com.sun.istack.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.WatchService;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by darren on 10/12/15.
 */
public class RemoteCachingFileSystem extends FileSystem
        implements Comparable<RemoteCachingFileSystem> {

    private final RemoteCachingFileSystemProvider provider;
    private final RemoteFileStore fileStore;
    private final SqarRemoteFileStore sqarFileStore;
    private final LruCache lruCache;
    private final FileTracker localOnlyFiles;

    public RemoteCachingFileSystem(final RemoteCachingFileSystemProvider provider,
                                   String name,
                                   Map<String, String> env) throws
            URISyntaxException,
            SQLException,
            ClassNotFoundException,
            IOException {
        this.provider = provider;
        this.fileStore = loadFileStore(env.get("remote-type"), env);
        this.sqarFileStore = new SqarRemoteFileStore(fileStore, env);
        this.localOnlyFiles = new FileTracker(Paths.get(new URI(env.get("local-tracking-root-uri"))));
        this.lruCache = new LruCache(Paths.get(new URI(env.get("cache-root-uri"))),
                                     Integer.parseInt(env.get("reservationSize")),
                                     Long.parseLong(env.get("cacheSize")),
                                     localOnlyFiles,
                                     new LruCache.Loader() {
                                         @Override
                                         public void load(RemoteCachingPath path,
                                                          Path tmpPath) throws IOException {
                                             if (SqarManager.isSqar(path, fileStore)) {
                                                 sqarFileStore.downloadFile(path, tmpPath);
                                             } else {
                                                 fileStore.downloadFile(path, tmpPath);
                                             }
                                         }

                                         @Override
                                         public long getFileLen(RemoteCachingPath path) throws
                                                                                        IOException {
                                             final RemoteFileStore.RemoteFileInfo rfi;

                                             if (SqarManager.isSqar(path, fileStore)) {
                                                 rfi = sqarFileStore.readInfo(path, true);
                                             } else {
                                                 rfi = fileStore.readInfo(path, true);
                                             }

                                             if (rfi != null && rfi.isFile)
                                                 return rfi.size;
                                             else
                                                 return -1;
                                         }
                                     });
    }

    private static FileSystemProvider getFileProvider(Path path) {
        return path.getFileSystem().provider();
    }

    private static RemoteFileStore loadFileStore(String type, Map<String, String> env) throws
            URISyntaxException,
            IOException {
        if ("s3".equals(type)) {
            return new S3RemoteFileStore(env);
        }
        if ("hdfs".equals(type)) {
            return new HdfsRemoteFileStore(env);
        }
        if ("local".equals(type)) {
            final URI root = new URI(env.get("local-filestore-root-uri"));
            return new LocalFileStore(Paths.get(root));
        }

        throw new RuntimeException("Unknown file store type: " + type);
    }

    @Override
    public int compareTo(@NotNull RemoteCachingFileSystem o) {
        return 0;
    }

    void createDirectory(RemoteCachingPath path, FileAttribute<?>[] attrs) throws IOException {
        if (lruCache.isPresent(path)) {
                throw new FileAlreadyExistsException("Already exists: " + path.toString());
        } else {
            /* check if a file exists remotely */
            final RemoteFileStore.RemoteFileInfo info = fileStore.readInfo(path);
            if (info != null) {
                if (info.isFile) {
                    /* file exists remotely */
                    throw new FileAlreadyExistsException("File with same name exists remotely: "
                                                                 + path.toString());
                } else {
                    /* directory exists remotely. */
                    throw new FileAlreadyExistsException("Directory exists remotely: "
                                                                 + path.toString());
                }
            }

            /* track directory locally */
            localOnlyFiles.addDirectory(path, attrs);
        }
    }

    private LruCache.FileSizeWatcher getFileSizeWatcher(RemoteCachingPath path,
                                                        Set<? extends OpenOption> options) throws
                                                                                           IOException {
        final LruCache.FileSizeWatcher watcher;

        /* check if the file is local only */
        if (lruCache.isPresent(path)) {
            if (!path.isLocalOnly()) {
                /* remote files are read only */
                throw new IOException("File " + path.toString() + " is read only");
            }
            watcher = lruCache.getAndOpenForWrite(path);
        } else {
            final RemoteFileStore.RemoteFileInfo info = fileStore.readInfo(path);
            if (info != null) {
                if (info.isFile) {
                    /* remote files are read only */
                    throw new IOException("File " + path.toString() + " is read only");
                } else {
                    throw new IOException("Cannot write to a directory: " + path.toString());
                }
            }
            watcher = lruCache.addNewFile(path, options.contains(StandardOpenOption.CREATE_NEW));
        }

        return watcher;
    }

    private WatcherAndCachePath getWatcherAndCachePath(RemoteCachingPath path,
                                                       Set<? extends OpenOption> options) throws
                                                                                          IOException {
        final WatcherAndCachePath results = new WatcherAndCachePath();

        if (options.contains(StandardOpenOption.WRITE) || options
                .contains(StandardOpenOption.APPEND)) {
            results.watcher = getFileSizeWatcher(path, options);
            results.cachePath = results.watcher.getPath();
        } else {
            /* only read the file */
            results.watcher = null;
            results.cachePath = lruCache.getAndOpenForRead(path);
        }
        return results;
    }

    boolean isDirectory(RemoteCachingPath path) throws IOException {
        final ImhotepFileAttributes attrs = lookupAttributes(path, true);

        return attrs != null && attrs.isDirectory();
    }

    Iterator<Path> iteratorOf(RemoteCachingPath dir,
                              DirectoryStream.Filter<? super Path> filter) throws IOException {
        final ArrayList<Path> results = new ArrayList<>();
        final ArrayList<RemoteFileStore.RemoteFileInfo> infos;

        switch (dir.getType()) {
            case ROOT:
                infos = fileStore.listDir(dir);
                break;
            case INDEX:
                infos = fileStore.listDir(dir);
                /* remove .sqar from directories */
                for (RemoteFileStore.RemoteFileInfo info : infos) {
                    if (!info.isFile && SqarManager.isSqarDir(info.path)) {
                        info.path = SqarManager.decodeShardName(info.path);
                    }
                }
                break;
            case SHARD:
            case FILE:
                if (SqarManager.isSqar(new RemoteCachingPath(this, dir.getShardPath()),
                                       this.fileStore)) {
                    infos = sqarFileStore.listDir(dir);
                } else {
                    infos = fileStore.listDir(dir);
                }
                break;
            default:
                throw new IllegalArgumentException();
        }

        for (RemoteFileStore.RemoteFileInfo info : infos) {
            final RemoteCachingPath path = new RemoteCachingPath(this, info.path);
            if (filter.accept(path)) {
                results.add(path);
            }
        }

        // add local files and directories
        final CloseableIterator<RemoteCachingPath> iter = localOnlyFiles.listDirectory(dir);
        while (iter.hasNext()) {
            final RemoteCachingPath path = iter.next();
            if (filter.accept(path)) {
                results.add(path);
            }
        }
        try {
            iter.close();
        } catch (IOException e) {
            // ignore
        }

        return results.iterator();
    }

    ImhotepFileAttributes lookupAttributes(RemoteCachingPath path) throws IOException {
        return lookupAttributes(path, false);
    }

    ImhotepFileAttributes lookupAttributes(RemoteCachingPath path, boolean onlyDirectories) throws
            IOException {
        if (localOnlyFiles.contains(path)) {
            return localOnlyFiles.getAttributes(path);
        }

        final RemoteFileStore.RemoteFileInfo info;
        if (SqarManager.isSqar(path, fileStore)) {
            if (onlyDirectories) {
                info = sqarFileStore.readInfo(path, false);
            } else {
                info = sqarFileStore.readInfo(path);
            }
        } else {
            if (onlyDirectories) {
                info = fileStore.readInfo(path, false);
            } else {
                info = fileStore.readInfo(path);
            }
        }

        if (info == null) {
            return null;
        }

        path.initAttributes(info.size, info.isFile, false);
        return path.getAttributes();
    }

    SeekableByteChannel newByteChannel(final RemoteCachingPath path,
                                       Set<? extends OpenOption> options,
                                       FileAttribute<?>... attrs) throws IOException {
        final SeekableByteChannel byteChannel;
        final WatcherAndCachePath watcherAndCachePath;
        final LruCache.FileSizeWatcher watcher;
        final Path cachePath;

        watcherAndCachePath = getWatcherAndCachePath(path, options);
        cachePath = watcherAndCachePath.cachePath;
        watcher = watcherAndCachePath.watcher;
        byteChannel = Files.newByteChannel(cachePath, options, attrs);
        return new CloseHookedByteChannel(byteChannel, watcher, new Runnable() {
            @Override
            public void run() {
                lruCache.markFileClosed(path);
            }
        });
    }

    FileChannel newFileChannel(final RemoteCachingPath path,
                               Set<? extends OpenOption> options,
                               FileAttribute<?>... attrs) throws IOException {
        final FileChannel fileChannel;
        final WatcherAndCachePath watcherAndCachePath;
        final LruCache.FileSizeWatcher watcher;
        final Path cachePath;

        watcherAndCachePath = getWatcherAndCachePath(path, options);
        cachePath = watcherAndCachePath.cachePath;
        watcher = watcherAndCachePath.watcher;
        fileChannel = getFileProvider(cachePath).newFileChannel(cachePath, options, attrs);
        return new CloseHookedFileChannel(fileChannel, watcher, new Runnable() {
            @Override
            public void run() {
                lruCache.markFileClosed(path);
            }
        });
    }

    InputStream newInputStream(final RemoteCachingPath path) throws IOException {
        final InputStream inputStream;
        final Path cachePath;

        cachePath = lruCache.getAndOpenForRead(path);
        inputStream = getFileProvider(cachePath).newInputStream(cachePath, StandardOpenOption.READ);
        return new CloseHookedInputStream(inputStream, new Runnable() {
            @Override
            public void run() {
                lruCache.markFileClosed(path);
            }
        });
    }

    OutputStream newOutputStream(final RemoteCachingPath path, OpenOption... optionsArr) throws
                                                                                         IOException {
        final OutputStream outputStream;
        final WatcherAndCachePath watcherAndCachePath;
        final LruCache.FileSizeWatcher watcher;
        final Path cachePath;
        final Set<OpenOption> options = new TreeSet<>();

        Collections.addAll(options, optionsArr);
        options.add(StandardOpenOption.WRITE);

        watcherAndCachePath = getWatcherAndCachePath(path, options);
        cachePath = watcherAndCachePath.cachePath;
        watcher = watcherAndCachePath.watcher;
        outputStream = getFileProvider(cachePath).newOutputStream(cachePath, optionsArr);
        return new CloseHookedOutputStream(outputStream, watcher, new Runnable() {
            @Override
            public void run() {
                lruCache.markFileClosed(path);
            }
        });
    }

    File getCacheFile(final RemoteCachingPath path) throws IOException {
        final Path cachePath;

        cachePath = lruCache.getAndOpenForRead(path);
        lruCache.markFileClosed(path);   // unable to track closes on a file, so don't try
        return cachePath.toFile();
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return RemoteCachingPath.PATH_SEPARATOR_STR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        ArrayList<Path> pathArr = new ArrayList<>();
        pathArr.add(new RemoteCachingPath(this, RemoteCachingPath.PATH_SEPARATOR_STR));
        return pathArr;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        ArrayList<FileStore> list = new ArrayList<>(1);
        list.add(fileStore);
        return list;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return ImmutableSet.of("basic");
    }

    @Override
    public Path getPath(String first, String... more) {
        if (more.length == 0) {
            return new RemoteCachingPath(this, first);
        }
        /* concat all strings into 1 byte array */
        /* calculate final array size */
        int len = 0;
        for (String s : more) {
            len += s.length();
        }

        /* concat strings */
        final StringBuilder path = new StringBuilder(len);
        path.append(first);
        for (final String s : more) {
            path.append(s);
        }
        return new RemoteCachingPath(this, path.toString());
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException();
    }

/////////////////////////////////////////////////////////////////////////////////////////

    private static class CloseHookedByteChannel implements SeekableByteChannel {
        private final SeekableByteChannel source;
        private final LruCache.FileSizeWatcher watcher;
        private final Runnable runnable;

        private CloseHookedByteChannel(SeekableByteChannel source1,
                                       LruCache.FileSizeWatcher watcher,
                                       Runnable runnable) {
            this.source = source1;
            this.watcher = watcher;
            this.runnable = runnable;
        }

        @Override
        public boolean isOpen() {
            return source.isOpen();
        }

        @Override
        public void close() throws IOException {
            watcher.trackClose();
            runnable.run();
            source.close();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return source.read(dst);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            final long pos = source.position();
            final long sz = source.size();
            final long appendAmt = pos + src.remaining() - sz;
            if (appendAmt > 0) {
                watcher.trackAppend(appendAmt);
            }
            return source.write(src);
        }

        @Override
        public long position() throws IOException {
            return source.position();
        }

        @Override
        public SeekableByteChannel position(long newPosition) throws IOException {
            return source.position(newPosition);
        }

        @Override
        public long size() throws IOException {
            return source.size();
        }

        @Override
        public SeekableByteChannel truncate(long size) throws IOException {
            return source.truncate(size);
        }
    }

    private static class CloseHookedFileChannel extends FileChannel {
        private final FileChannel source;
        private final Runnable runnable;

        private CloseHookedFileChannel(FileChannel source,
                                       LruCache.FileSizeWatcher watcher,
                                       Runnable runnable) {
            this.source = source;
            this.runnable = runnable;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            runnable.run();
            source.close();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return source.read(dst);
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            return source.read(dsts, offset, length);
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return source.write(src);
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            return source.write(srcs, offset, length);
        }

        @Override
        public long position() throws IOException {
            return source.position();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            return source.position(newPosition);
        }

        @Override
        public long size() throws IOException {
            return source.size();
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            return source.truncate(size);
        }

        @Override
        public void force(boolean metaData) throws IOException {
            source.force(metaData);
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws
                                                                                      IOException {
            return source.transferTo(position, count, target);
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws
                                                                                     IOException {
            return source.transferFrom(src, position, count);
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return source.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            return source.write(src, position);
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            return source.map(mode, position, size);
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            return source.lock(position, size, shared);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            return source.tryLock(position, size, shared);
        }
    }

    private static class CloseHookedInputStream extends InputStream {
        private final InputStream source;
        private final Runnable runnable;

        private CloseHookedInputStream(InputStream source, Runnable runnable) {
            this.source = source;
            this.runnable = runnable;
        }

        @Override
        public int read() throws IOException {
            return source.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return source.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return source.read(b, off, len);
        }

        @Override
        public long skip(long n) throws IOException {
            return source.skip(n);
        }

        @Override
        public int available() throws IOException {
            return source.available();
        }

        @Override
        public void close() throws IOException {
            runnable.run();
            source.close();
        }

        @Override
        public void mark(int readlimit) {
            source.mark(readlimit);
        }

        @Override
        public void reset() throws IOException {
            source.reset();
        }

        @Override
        public boolean markSupported() {
            return source.markSupported();
        }
    }

    private static class CloseHookedOutputStream extends OutputStream {
        private final OutputStream source;
        private final LruCache.FileSizeWatcher watcher;
        private final Runnable runnable;

        public CloseHookedOutputStream(OutputStream source,
                                       LruCache.FileSizeWatcher watcher,
                                       Runnable runnable) {
            this.source = source;
            this.watcher = watcher;
            this.runnable = runnable;
        }

        @Override
        public void write(int b) throws IOException {
            watcher.trackAppend(4);
            source.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            watcher.trackAppend(b.length);
            source.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            watcher.trackAppend(len);
            source.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            source.flush();
        }

        @Override
        public void close() throws IOException {
            runnable.run();
            source.close();
        }
    }

    private static final class WatcherAndCachePath {
        private LruCache.FileSizeWatcher watcher;
        private Path cachePath;
    }

}
