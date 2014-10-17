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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * @author jsgroth
 */
public class CopyDirectoryToLocal {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("ARGS: from to");
            System.exit(1);
        }

        System.out.println("Copying archives from " + args[0] + " to " + args[1]);
        copy(new Path(args[0]), new File(args[1]));
        System.out.println("Wrote to: " + args[1]);
    }

    public static void copy(Path from, File to) throws IOException {
        copy(from.getFileSystem(new Configuration()), from, to);
    }

    public static void copy(FileSystem fs, Path from, File to) throws IOException {
        if ((to.exists() && !to.isDirectory())) {
            throw new FileNotFoundException(to.getAbsolutePath() + " is not a directory");
        } else if (!to.exists() && !to.mkdirs()) {
            throw new IOException("unable to create directory " + to.getAbsolutePath()); 
        }

        for (final FileStatus status : fs.listStatus(from)) {
            if (status.isDir()) {
                final Path path = status.getPath();
                System.out.println("copying " + path);
                final String pathName = path.getName();
                final String dirName = pathName.endsWith(".sqar") ? pathName.substring(0, pathName.length() - 5) : pathName;
                new SquallArchiveReader(fs, path).copyAllToLocal(new File(to, dirName));
            }
        }
    }
}
