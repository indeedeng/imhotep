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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * @author jsgroth
 */
public class CopyToLocal {
    private CopyToLocal() {
    }

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("ARGS: fromShard1 fromShard2 fromShard3... toParentDir");
            System.exit(1);
        }

        final File toParentDir = new File(args[args.length-1]);

        for(int i = 0; i < args.length - 1; i ++) {
            final Path from = new Path(args[i]);
            final File to = new File(toParentDir, from.getName().replace(".sqar", ""));
            copy(from, to);
            System.out.println("Downloaded " + from + " to " + to);
        }

    }

    public static void copy(final Path from, final File to) throws IOException {
        copy(from.getFileSystem(new Configuration()), from, to);
    }

    public static void copy(final FileSystem fs, final Path from, final File to) throws IOException {
        new SquallArchiveReader(fs, from).copyAllToLocal(to);
    }
}
