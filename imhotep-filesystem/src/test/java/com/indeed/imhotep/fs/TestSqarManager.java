package com.indeed.imhotep.fs;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.commons.collections.IteratorUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by darren on 12/11/15.
 */
public class TestSqarManager {
    private static Map<String, String> testSettings;

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {

        Files.createDirectories(Paths.get(new URI("file:///tmp/cache")));
        Files.createDirectories(Paths.get(new URI("file:///tmp/tracking")));

        testSettings = new HashMap<>();

        testSettings.put("sqlite-max-mem", "50");
        testSettings.put("database-location", "/tmp/h2");

        testSettings.put("s3-bucket", "");
        testSettings.put("s3-prefix", "");
        testSettings.put("s3-key", "");
        testSettings.put("s3-secret", "");


        testSettings.put("local-filestore-root-uri", "file:///tmp/data");

        testSettings.put("remote-type", "local");
        testSettings.put("local-tracking-root-uri", "file:///tmp/tracking");
        testSettings.put("cache-root-uri", "file:///tmp/cache");
        testSettings.put("reservationSize", "16000");
        testSettings.put("cacheSize", Long.toString(100 * 1024 * 1024));

        FileSystems.newFileSystem(new URI("rcfs:/foo/"), testSettings);
    }

    @AfterClass
    public static void cleanup() throws IOException, URISyntaxException {
        final RemovalVistor removalVistor = new RemovalVistor();

        Files.walkFileTree(Paths.get(new URI("file:///tmp/cache")), removalVistor);
        Files.walkFileTree(Paths.get(new URI("file:///tmp/tracking")), removalVistor);
        Files.delete(Paths.get(new URI("file:///tmp/h2.mv.db")));
    }

    static class RemovalVistor extends SimpleFileVisitor<Path> {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
        }

    }


    @Test
    public void testSqarExists() throws IOException, URISyntaxException {
        boolean result;
        Path target;

        target = Paths.get(new URI("rcfs:/testData/"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3/"));
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1"));
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3/4/5/"));
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3/4/5/12345.file"));
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.isRegularFile(target);
        assertTrue(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3/4/1234.file"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.isRegularFile(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/3/4/5/345.file"));
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.isRegularFile(target);
        assertTrue(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/test-archive/4/5/45.file"));
        result = Files.isRegularFile(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.exists(target);
        assertTrue(result);


        target = Paths.get(new URI("rcfs:/testData/test-archive/6"));
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.exists(target);
        assertFalse(result);
    }

    @Test
    public void testSqarListDirectory() throws IOException, URISyntaxException {
        DirectoryStream<Path> result;
        Path target;
        final String[] dir0 = {"1", "2", "3", "4", "5", "test-archive"};
        final String[] dir123 = {"4", "123.file"};
        final String[] dir1 = {"1", "2", "1.file"};
        String[] dir11 = new String[2502];
        boolean success;

        for (int i = 0; i < 2500; i++) {
            dir11[i] = Integer.toString(i) + ".file";
        }
        dir11[2500] = "2";
        dir11[2501] = "3";

        Arrays.sort(dir123);
        Arrays.sort(dir1);
        Arrays.sort(dir11);

        target = Paths.get(new URI("rcfs:/testData"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir0);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir123);
        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3/"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir123);
        target = Paths.get(new URI("rcfs:/testData/test-archive/1"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir1);
        target = Paths.get(new URI("rcfs:/testData/test-archive/1/"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir1);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3/4/5/12345.file"));
        try{
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);
        target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3/4/1234.file"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);
        target = Paths.get(new URI("rcfs:/testData/test-archive/3/4/5/345.file"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);
        target = Paths.get(new URI("rcfs:/testData/test-archive/4/5/45.file"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);
        target = Paths.get(new URI("rcfs:/testData/test-archive/6"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);

        target = Paths.get(new URI("rcfs:/testData/test-archive/1/1/"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir11);
    }

    private void verifyIterator(Iterable<Path> iterable, String[] validData) {
        final Iterable<String> stringIterable;
        final Iterator<String> iter;
        final List<String> iterData;

        stringIterable = Iterables.transform(iterable, new Function<Path, String>() {
            @Nullable
            @Override
            public String apply(@Nullable Path input) {
                return input.toString();
            }
        });
        iter = stringIterable.iterator();
        if (validData.length > 0) 
            assertTrue(iter.hasNext());
        else
            assertFalse(iter.hasNext());
        
        iterData = IteratorUtils.toList(iter);
        Arrays.sort(validData);
        Collections.sort(iterData);
        assertArrayEquals(validData, iterData.toArray(new String[iterData.size()]));
    }

    @Test
    public void testSqarLoadFile() throws IOException, URISyntaxException {
        InputStream result = null;
        Path target;
        boolean sawException;

        sawException = false;
        result = null;
        try {
            target = Paths.get(new URI("rcfs:/testData/test-archive/1/2/3"));
            result = Files.newInputStream(target);
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = Paths.get(new URI("rcfs:/testData/test-archive/1"));
            result = Files.newInputStream(target);
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = Paths.get(new URI("rcfs:/testData/test-archive/1/"));
            result = Files.newInputStream(target);
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        testInputStream("rcfs:/testData/test-archive/1/2/3/4/5/12345.file", 12345);
        testInputStream("rcfs:/testData/test-archive/1/2/3/4/1234.file", 1234);
        testInputStream("rcfs:/testData/test-archive/3/4/5/345.file", 345);
        testInputStream("rcfs:/testData/test-archive/4/5/45.file", 45);
    }

    @Test
    public void testExists() throws IOException, URISyntaxException {
        boolean result;
        Path target;

        target = Paths.get(new URI("rcfs:/testData/1/2/3"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData/1/2/3/"));
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/1"));
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/1/"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);

        target = Paths.get(new URI("rcfs:/testData/1/2/3/4/5/"));
        result = Files.isDirectory(target);
        assertTrue(result);
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/1/2/3/4/5/12345.file"));
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.isRegularFile(target);
        assertTrue(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/1/2/3/4/1234.file"));
        result = Files.exists(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.isRegularFile(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/3/4/5/345.file"));
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.isRegularFile(target);
        assertTrue(result);
        result = Files.exists(target);
        assertTrue(result);

        target = Paths.get(new URI("rcfs:/testData/4/5/45.file"));
        result = Files.isRegularFile(target);
        assertTrue(result);
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.exists(target);
        assertTrue(result);


        target = Paths.get(new URI("rcfs:/testData/6"));
        result = Files.isRegularFile(target);
        assertFalse(result);
        result = Files.isDirectory(target);
        assertFalse(result);
        result = Files.exists(target);
        assertFalse(result);
    }

    @Test
    public void testLoadFile() throws IOException, URISyntaxException {
        InputStream result = null;
        Path target;
        boolean sawException;

        sawException = false;
        result = null;
        try {
            target = Paths.get(new URI("rcfs:/testData/1/2/3"));
            result = Files.newInputStream(target);
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = Paths.get(new URI("rcfs:/testData/1"));
            result = Files.newInputStream(target);
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = Paths.get(new URI("rcfs:/testData/1/"));
            result = Files.newInputStream(target);
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        testInputStream("rcfs:/testData/1/2/3/4/5/12345.file", 12345);
        testInputStream("rcfs:/testData/1/2/3/4/1234.file", 1234);
        testInputStream("rcfs:/testData/3/4/5/345.file", 345);
        testInputStream("rcfs:/testData/4/5/45.file", 45);
    }

    private void testInputStream(String uri, int len) throws URISyntaxException {
        boolean sawException;
        InputStream result;
        Path target;
        sawException = false;
        result = null;
        try {
            target = Paths.get(new URI(uri));
            result = Files.newInputStream(target);

            int data;
            InputStreamReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i < len; i++) {
                    expected += "foo!";
                }

                fis = new InputStreamReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert (data == expected.charAt(i));
                }
                assert (fis.read() == -1);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            sawException = true;
        }
        assertNotNull(result);
        assertFalse(sawException);
    }

    @Test
    public void testListDirectory() throws IOException, URISyntaxException {
        DirectoryStream<Path> result;
        Path target;
        String[] dir123 = {"4", "123.file"};
        String[] dir1 = {"1", "2", "1.file"};
        String[] dir11 = new String[2502];
        boolean success;

        for (int i = 0; i < 2500; i++) {
            dir11[i] = Integer.toString(i) + ".file";
        }
        dir11[2500] = "2";
        dir11[2501] = "3";

        Arrays.sort(dir123);
        Arrays.sort(dir1);
        Arrays.sort(dir11);

        target = Paths.get(new URI("rcfs:/testData/1/2/3"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir123);
        target = Paths.get(new URI("rcfs:/testData/1/2/3/"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir123);
        target = Paths.get(new URI("rcfs:/testData/1"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir1);
        target = Paths.get(new URI("rcfs:/testData/1/"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir1);

        target = Paths.get(new URI("rcfs:/testData/1/2/3/4/5/12345.file"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);

        target = Paths.get(new URI("rcfs:/testData/1/2/3/4/1234.file"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);

        target = Paths.get(new URI("rcfs:/testData/3/4/5/345.file"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);

        target = Paths.get(new URI("rcfs:/testData/4/5/45.file"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);

        target = Paths.get(new URI("rcfs:/testData/6"));
        try {
            result = Files.newDirectoryStream(target);
            success = false;
        } catch (NotDirectoryException e) {
            success = true;
        }
        assertTrue(success);


        target = Paths.get(new URI("rcfs:/testData/1/1/"));
        result = Files.newDirectoryStream(target);
        verifyIterator(result, dir11);

    }

    @Test
    public void testCacheRemove() throws IOException {
//        client.putObject(s3bucket, "1/1/2/112.file", dataFile);
//        client.putObject(s3bucket, "1/1/3/113.file", dataFile);
    }


}
