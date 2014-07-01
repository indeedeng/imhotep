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
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("ARGS: from to");
            System.exit(1);
        }

        copy(new Path(args[0]), new File(args[1]));
        System.out.println("Wrote: " + args[1]);
    }

    public static void copy(Path from, File to) throws IOException {
        copy(from.getFileSystem(new Configuration()), from, to);
    }

    public static void copy(FileSystem fs, Path from, File to) throws IOException {
        new SquallArchiveReader(fs, from).copyAllToLocal(to);
    }
}
