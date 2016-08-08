package com.indeed.imhotep.controller;

import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.fs.SqarManager;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by darren on 2/29/16.
 */
public class ScanImhotepShardSizes {

    private static Map<String, String> testSettings;
    private static RemoteFileScanner rfs;

    @BeforeClass
    public static void init() throws
            IOException,
            URISyntaxException,
            SQLException,
            ClassNotFoundException {

        Files.createDirectories(Paths.get(new URI("file:/tmp/cache")));
        Files.createDirectories(Paths.get(new URI("file:/tmp/tracking")));

        testSettings = new HashMap<>();

        testSettings.put("sqlite-max-mem", "500");
        testSettings.put("database-location", "/tmp/3ndTry");
        testSettings.put("hdfs-base-path", "/var/imhotep");

        testSettings.put("remote-type", "hdfs");
        testSettings.put("local-tracking-root-uri", "file:///tmp/tracking");
        testSettings.put("cache-root-uri", "file:///tmp/cache");
        testSettings.put("reservationSize", "16000");
        testSettings.put("cacheSize", Long.toString(100 * 1024 * 1024));

        FileSystems.newFileSystem(new URI("rcfs:/foo/"), testSettings);

        rfs = new RemoteFileScanner(testSettings);

    }

    @Test
    public void testScanRemoteFiles() throws
            IOException,
            URISyntaxException,
            SQLException,
            InterruptedException {
        rfs.scanRemoteFiles("organic", "sponsored", "mobileorganic", "mobilesponsored");
    }

}
