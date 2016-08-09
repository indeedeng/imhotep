package com.indeed.imhotep.fs;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;

/**
 * @author darren
 */
class LocalFileStore extends RemoteFileStore {
    private final Path root;

    LocalFileStore(final Path root) {
        this.root = root;
    }

    private LocalFileStore(final Map<String, ?> configuration) throws URISyntaxException {
        this(Paths.get(new URI((String) configuration.get("local-filestore.root-uri"))));
    }

    @Override
    public List<RemoteFileInfo> listDir(final RemoteCachingPath path) throws IOException {
        try (final DirectoryStream<Path> dirStream = Files.newDirectoryStream(getLocalPath(path))) {

            return FluentIterable.from(dirStream).transform(new Function<Path, RemoteFileInfo>() {
                @Override
                public RemoteFileInfo apply(final Path localPath) {
                    try {
                        final BasicFileAttributes attributes = Files.readAttributes(localPath, BasicFileAttributes.class);
                        return new RemoteFileInfo(localPath.relativize(root).toString(),
                                attributes.size(),
                                !attributes.isDirectory());
                    } catch (final IOException e) {
                        throw new RuntimeException("Failed to get attributes for " + localPath, e);
                    }
                }
            }).toList();
        }
    }

    @Override
    public String name() {
        return root.toString();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public RemoteFileInfo readInfo(final String shardPath) throws IOException {
        final Path localPath = getLocalPath(shardPath);
        final BasicFileAttributes attributes = Files.readAttributes(localPath, BasicFileAttributes.class);
        return new RemoteFileInfo(shardPath, attributes.size(), !attributes.isDirectory());
    }

    @Override
    public void downloadFile(final RemoteCachingPath srcPath, final Path destPath) throws IOException {
        Files.copy(getLocalPath(srcPath), destPath, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public InputStream getInputStream(final String path, final long startOffset, final long length) throws
            IOException {
        final Path localPath = getLocalPath(path);
        final InputStream is = Files.newInputStream(localPath);
        if (is.skip(startOffset) != startOffset) {
            throw new IOException("Could not move offset for path " + path + " by " + startOffset);
        }
        return is;
    }

    private Path getLocalPath(final String path) {
        final String relPath;

        if (path.startsWith(RemoteCachingPath.PATH_SEPARATOR_STR)) {
            relPath = path.substring(RemoteCachingPath.PATH_SEPARATOR_STR.length());
        } else {
            relPath = path;
        }

        return root.resolve(relPath);
    }

    private Path getLocalPath(final RemoteCachingPath path) {
        return root.resolve(path.relativize(path.getRoot()).toString());
    }

    static class Builder implements RemoteFileStore.Builder {
        @Override
        public RemoteFileStore build(final Map<String, ?> configuration) {
            try {
                return new LocalFileStore(configuration);
            } catch (final URISyntaxException e) {
                throw new IllegalArgumentException("Failed to initialize local file store", e);
            }
        }
    }
}
