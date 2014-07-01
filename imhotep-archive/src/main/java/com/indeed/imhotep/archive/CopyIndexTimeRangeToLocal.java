package com.indeed.imhotep.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jplaisance
 */
public final class CopyIndexTimeRangeToLocal {

    static {
        DateTimeZone.setDefault(DateTimeZone.forOffsetHours(-6));
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 4) {
            System.err.println("ARGS: from to start end");
            System.exit(1);
        }

        System.out.println("Copying archives from " + args[0] + " to " + args[1]);
        copy(new Path(args[0]), new File(args[1]), new DateTime(args[2]), new DateTime(args[3]));
        System.out.println("Wrote to: " + args[1]);
    }

    public static void copy(Path from, File to, DateTime start, DateTime end) throws IOException {
        copy(from.getFileSystem(new Configuration()), from, to, start, end);
    }

    private static final Pattern shardPattern = Pattern.compile("index([0-9]{4})([0-9]{2})([0-9]{2})\\.([0-9]{2})\\.[0-9]{14}\\.sqar");

    public static void copy(FileSystem fs, Path from, File to, final DateTime start, final DateTime end) throws IOException {
        if ((to.exists() && !to.isDirectory())) {
            throw new FileNotFoundException(to.getAbsolutePath() + " is not a directory");
        } else if (!to.exists() && !to.mkdirs()) {
            throw new IOException("unable to create directory " + to.getAbsolutePath());
        }
        for (final FileStatus status : fs.listStatus(from, new PathFilter() {
            public boolean accept(final Path path) {
                final Matcher m = shardPattern.matcher(path.getName());
                if (!m.matches()) {
                    return false;
                }
                final DateTime dateTime = new DateTime(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3)), Integer.parseInt(m.group(4)), 0);
                return dateTime.getMillis() >= start.getMillis() && dateTime.getMillis() < end.getMillis();
            }
        })) {
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
