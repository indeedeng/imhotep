package com.indeed.imhotep.io.caching.RemoteCaching;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.collections.IteratorUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by darren on 12/29/15.
 */
public class LocalFileStore extends RemoteFileStore {
    private final Path root;

    public LocalFileStore(Path root) {
        this.root = root;
    }

    @Override
    public ArrayList<RemoteFileInfo> listDir(RemoteCachingPath path) throws IOException {
        final Path localPath = getLocalPath(path);
        final DirectoryStream<Path> dirStream = Files.newDirectoryStream(localPath);
        final Iterable<RemoteFileInfo> iterable;
        final List<RemoteFileInfo> results;

        iterable = Iterables.transform(dirStream, new Function<Path, RemoteFileInfo>() {
            @Nullable
            @Override
            public RemoteFileInfo apply(@Nullable Path input) {
                final BasicFileAttributes attributes;

                try {
                    attributes = Files.readAttributes(input, BasicFileAttributes.class);
                    return new RemoteFileInfo(input.toString(),
                                              attributes.size(),
                                              !attributes.isDirectory());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        results = IteratorUtils.toList(iterable.iterator());
        if (results instanceof ArrayList)
            return (ArrayList)results;
        else
            return new ArrayList<>(results);
    }

    @Override
    public String name() {
        return "Local File Store";
    }

    @Override
    public String type() {
        return "Remote File Store";
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
        return false;
    }

    @Override
    public boolean supportsFileAttributeView(String name) {
        return false;
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
        return null;
    }

    @Override
    public Object getAttribute(String attribute) throws IOException {
        return null;
    }

    @Override
    public RemoteFileInfo readInfo(String shardPath) throws IOException {
        final Path localPath = getLocalPath(shardPath);
        final BasicFileAttributes attributes = Files.readAttributes(localPath,
                                                                    BasicFileAttributes.class);

        return new RemoteFileInfo(shardPath, attributes.size(), !attributes.isDirectory());
    }

    @Override
    public void downloadFile(RemoteCachingPath path, Path tmpPath) throws IOException {
        final Path localPath = getLocalPath(path);

        Files.copy(localPath, tmpPath);
    }

    @Override
    public InputStream getInputStream(String path, long startOffset, long length) throws
                                                                                  IOException {
        final Path localPath = getLocalPath(path);
        final InputStream is = Files.newInputStream(localPath);
        is.skip(startOffset);
        return is;
    }

    private Path getLocalPath(String path) {
        final String relPath = path.substring(RemoteCachingPath.PATH_SEPARATOR_STR.length());

        return root.resolve(relPath);
    }

    private Path getLocalPath(RemoteCachingPath path) {
        final Path relPath = path.relativize(path.getRoot());

        return root.resolve(relPath.toString());
    }

}
