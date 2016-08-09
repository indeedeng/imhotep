package com.indeed.imhotep.fs;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author kenh
 */

public class RemoteCachingFileSystem2 extends FileSystem {
    private final RemoteCachingFileSystemProvider2 provider;
    private final RemoteFileStore fileStore;

    public RemoteCachingFileSystem2(final RemoteCachingFileSystemProvider2 provider, final Map<String, ?> configuration) {
        this.provider = provider;
        fileStore = RemoteFileStoreType.fromName((String) configuration.get("remote-type"))
                .getBuilder().build(configuration);
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("This filesystem is not closeable");
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public String getSeparator() {
        return RemoteCachingPath.PATH_SEPARATOR_STR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return Collections.singletonList((Path) new RemoteCachingPath(this, RemoteCachingPath.PATH_SEPARATOR_STR));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return Collections.singletonList((FileStore) fileStore);
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return ImmutableSet.of("basic");
    }

    @Override
    public Path getPath(final String first, final String... more) {
        return new RemoteCachingPath(this, Joiner.on(RemoteCachingPath.PATH_SEPARATOR_STR).join(Lists.asList(first, more)));
    }

    @Override
    public PathMatcher getPathMatcher(final String syntaxAndPattern) {
        final int idx = syntaxAndPattern.indexOf(':');
        if ((idx == -1) && (idx >= (syntaxAndPattern.length() - 1))) {
            throw new IllegalArgumentException("Unexpected syntax and pattern format: " + syntaxAndPattern);
        }
        final String syntax = syntaxAndPattern.substring(0, idx);
        if ("regex".equals(syntax)) {
            final String pattern = syntaxAndPattern.substring(idx + 1);
            final Pattern compiledPattern = Pattern.compile(pattern);
            return new PathMatcher() {
                @Override
                public boolean matches(final Path path) {
                    return compiledPattern.matcher(path.toString()).matches();
                }
            };
        } else {
            throw new UnsupportedOperationException("Unsupported syntax " + syntax);
        }
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException();
    }
}
