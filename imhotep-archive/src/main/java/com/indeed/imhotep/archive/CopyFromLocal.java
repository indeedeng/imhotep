/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.archive;

import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * @author jsgroth
 */
public class CopyFromLocal {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("ARGS: from to [--overwrite]");
            System.exit(1);
        }

        boolean overwrite = false;
        for (int i = 2; i < args.length; ++i) {
            if (args[i].equals("--overwrite")) {
                overwrite = true;
            } else {
                throw new IllegalArgumentException("unrecognized arg: " + args[i]);
            }
        }

        copy(new File(args[0]), new Path(args[1]), overwrite);
    }

    public static void copy(File from, Path to, boolean overwrite) throws IOException {
        copy(to.getFileSystem(new Configuration()), from, to, overwrite);
    }

    public static void copy(FileSystem fs, File from, Path to, boolean overwrite) throws IOException {
        copy(fs, from, to, overwrite, SquallArchiveCompressor.GZIP);
    }

    public static void copy(FileSystem fs, File from, Path to, boolean overwrite, SquallArchiveCompressor compressor) throws IOException {
        if (fs.exists(to)) {
            if (!overwrite) {
                throw new IOException("path already exists: " + to);
            }
            fs.delete(to, true);
        }
        final SquallArchiveWriter w = new SquallArchiveWriter(fs, to, true, compressor);
        if (from.isDirectory()) {
            w.batchAppendDirectory(from);
        } else {
            w.appendFile(from);
            w.commit();            
        }
    }
}
