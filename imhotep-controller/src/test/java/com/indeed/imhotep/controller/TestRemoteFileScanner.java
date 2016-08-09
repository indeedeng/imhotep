package com.indeed.imhotep.controller;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by darren on 2/29/16.
 */
public class TestRemoteFileScanner {

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

        testSettings.put("sqlite-max-mem", "50");
        testSettings.put("database-location", "/tmp/h2db");

        rfs = new RemoteFileScanner(testSettings);
    }

    @Test
    public void testScanRemoteFiles() throws IOException, URISyntaxException, SQLException {
        rfs.scanRemoteFiles();
    }
}
