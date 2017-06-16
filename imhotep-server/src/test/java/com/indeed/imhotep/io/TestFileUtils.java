package com.indeed.imhotep.io;

import com.indeed.imhotep.client.ShardTimeUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by darren on 1/15/16.
 *
 */
public class TestFileUtils {

    private static final Logger log = Logger.getLogger(TestFileUtils.class);

    private static final List<Path> deleteOnExit = new ArrayList<>();
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                for(final Path path : deleteOnExit ) {
                    try {
                        deleteDirTree(path);
                    } catch(final IOException ex) {
                       log.error("Error while deleting " + path, ex);
                    }
                }
            }
        } ) );
    }

    private TestFileUtils() {
    }

    public static void deleteDirTree(final Path dir) throws IOException {
        Files.walkFileTree(dir, new RemovalVisitor());
    }

    private static class RemovalVisitor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
        }
    }

    public static boolean deleteDirTreeOnExit(final Path dir) {
        if(deleteOnExit.contains(dir)) {
            return false;
        }

        deleteOnExit.add(dir);
        return true;
    }

    public static Path createTempShard() {
        try {
            final DateTime shardTime = new DateTime(2017, 1, 1, 0, 0, 0);
            return createTempShard(null, shardTime, "");
        } catch (final IOException e) {
            return Paths.get(".");
        }
    }

    public static Path createTempShard(final Path rootDir, final DateTime date, final String suffix) throws IOException {

        final String shardName = ShardTimeUtils.toHourlyShardPrefix(date);
        final Path shardDir;
        if(rootDir == null) {
            shardDir = Files.createTempDirectory(shardName + suffix);
        } else {
            shardDir = Files.createTempDirectory(rootDir, shardName + suffix);
        }

        if(!deleteDirTreeOnExit(shardDir)) {
            log.warn("TestFileUtils: dir '" + shardDir + "' is already in autodelete list");
        }

        return shardDir;
    }
}
