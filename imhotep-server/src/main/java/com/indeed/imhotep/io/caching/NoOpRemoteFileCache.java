package com.indeed.imhotep.io.caching;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NoOpRemoteFileCache extends RemoteFileCache {

    public NoOpRemoteFileCache(Properties properties) {
    }

    @Override
    public File getFile(String path) throws ExecutionException {
        return null;
    }

    @Override
    public boolean contains(String path) {
        return false;
    }

}
