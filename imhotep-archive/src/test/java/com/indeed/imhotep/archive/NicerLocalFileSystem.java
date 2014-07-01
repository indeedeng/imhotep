package com.indeed.imhotep.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.net.URI;

/**
 * @author jsgroth
 *
 * a RawLocalFileSystem that throws exceptions if you try to call append
 * "nicer" because you don't have to explicitly call initialize after construction
 */
public class NicerLocalFileSystem extends RawLocalFileSystem {
    public NicerLocalFileSystem() throws IOException {
        initialize(URI.create("file:///"), new Configuration());
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException();
    }
}
