package com.indeed.imhotep.controller;

import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by darren on 2/26/16.
 */
public interface DirLister {
    RemoteIterator<String> listSubdirectories(String path) throws IOException;

    RemoteIterator<String> listFiles(String path) throws IOException;

    RemoteIterator<String> listFilesAndDirectories(String path) throws IOException;

    InputStream getInputStream(String path, long startOffset, long length) throws
            IOException;
}
