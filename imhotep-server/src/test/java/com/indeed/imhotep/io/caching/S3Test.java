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
 ///* This test is expensive and slow, so skip it unless there is a specific problem */
//
//
//package com.indeed.imhotep.io.caching;
//
//import static org.junit.Assert.*;
//
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import com.amazonaws.auth.BasicAWSCredentials;
//import com.amazonaws.services.s3.AmazonS3Client;
//
///**
// * Unit test for S3CachedFileSystem
// */
//public class S3Test {
//    private static String s3bucket;
//    private static AmazonS3Client client;
//    private static List<Map<String,Object>> testSettings;
//    
//    @BeforeClass
//    public static void init() throws IOException {
//        final Properties properties;
//        final InputStream in;
//        final String s3key;
//        final String s3secret;
//        final BasicAWSCredentials cred;
//        
//        properties = new Properties();
//        in = ClassLoader.getSystemResourceAsStream("s3-test.properties");
//        properties.load(in);
//        in.close();
//        
//        s3bucket = properties.getProperty("s3-bucket");
//        s3key = properties.getProperty("s3-key");
//        s3secret = properties.getProperty("s3-secret");
//        cred = new BasicAWSCredentials(s3key, s3secret);
//
//        client = new AmazonS3Client(cred);
//        
//        testSettings = new ArrayList<Map<String,Object>>();
//        
//        Map<String,Object> s3layer = new HashMap<String,Object>();
//        s3layer.put("type", "S3");
//        s3layer.put("order", 2);
//        s3layer.put("mountpoint", "/");
////        s3layer.put("s3-prefix", "/data");
//        s3layer.put("s3-bucket", s3bucket);
//        s3layer.put("s3-key", s3key);
//        s3layer.put("s3-secret", s3secret);
//        testSettings.add(s3layer);
//        
//        Map<String,Object> cachelayer = new HashMap<String,Object>();
//        cachelayer.put("type", "CACHED");
//        cachelayer.put("order", 3);
//        cachelayer.put("mountpoint", "/");
//        cachelayer.put("cache-dir", "/tmp/s3_test_temp_cache");
//        cachelayer.put("cacheSizeMB", 1);
//        testSettings.add(cachelayer);
//        
////        createTestData();
//
//        CachedFile.init(testSettings, "/foo/data", false);
//        
//    }
//    
//    public static void createTestData() throws IOException {
//        final String data = "foobar?!";
//        File dataFile;
//        FileWriter os;
//        
////        final String s3prefix = "data/";
//        final String s3prefix = "";
//        
//        for (int i = 5; i >= 1; i --) {
//            String key;
//            String path = "";
//            String filename = "";
//            for (int j = 1; j <= i; j++) {
//                path += Integer.toString(j) + "/";
//                filename += Integer.toString(j);
//            }
//
//            /* write data to file */
//            dataFile = File.createTempFile("S3Test", null);
//            os = new FileWriter(dataFile);
//            for (int j = 0; j < Integer.parseInt(filename); j++) {
//                os.write(data);
//            }
//            os.close();
//            
//            /* write data to s3 */
//            key = path + filename + ".file";
//            client.putObject(s3bucket, s3prefix + key, dataFile);
//            dataFile.delete();
//        }
//
//        /* write data to file */
//        dataFile = File.createTempFile("S3Test", null);
//        os = new FileWriter(dataFile);
//        for (int j = 0; j < 4; j++) {
//            os.write(data);
//        }
//        os.close();
//        /* to test directory lookups */
//        client.putObject(s3bucket, s3prefix + "1/2/31/1231.file", dataFile);
//        dataFile.delete();
//
//        for (int i = 2; i <= 5; i++) {
//            String key;
//            String path = "";
//            String filename = "";
//            for (int j = i; j <= 5; j++) {
//                path += Integer.toString(j) + "/";
//                filename += Integer.toString(j);
//            }
//            /* write data to file */
//            dataFile = File.createTempFile("S3Test", null);
//            os = new FileWriter(dataFile);
//            for (int j = 0; j < Integer.parseInt(filename); j++) {
//                os.write(data);
//            }
//            os.close();
//            
//            /* write data to s3 */
//            key = path + filename + ".file";
//            client.putObject(s3bucket, s3prefix + key, dataFile);
//            dataFile.delete();
//        }
//        
//        /* write data to file */
//        dataFile = File.createTempFile("S3Test", null);
//        os = new FileWriter(dataFile);
//        for (int j = 0; j < 4; j++) {
//            os.write(data);
//        }
//        os.close();
//
//        /* populate an path beginning with 1 to test list chunking */
//        String path = "1/1/";
//        for (int i = 0; i < 2500; i++) {
//            String key;
//            String filename = Integer.toString(i);
//            key = path + filename + ".file";
//            client.putObject(s3bucket, s3prefix + key, dataFile);
//        }
//        client.putObject(s3bucket, s3prefix + "1/1/2/112.file", dataFile);
//        client.putObject(s3bucket, s3prefix + "1/1/3/113.file", dataFile);
//        dataFile.delete();
//    }
//
//    
//    @Test
//    public void testExists() throws IOException {
//        boolean result;
//        CachedFile target;
//        
//        target = CachedFile.create("/foo/data");
//        result = target.isFile();
//        assertFalse(result);
//        result = target.isDirectory();
//        assertTrue(result);
//        result = target.exists();
//        assertTrue(result);
//
//        target = CachedFile.create("/foo/data/1/2/3");
//        result = target.exists();
//        assertTrue(result);
//        result = target.isDirectory();
//        assertTrue(result);
//        result = target.isFile();
//        assertFalse(result);
//        
//        target = CachedFile.create("/foo/data/1/2/3/");
//        result = target.isDirectory();
//        assertTrue(result);
//        result = target.isFile();
//        assertFalse(result);
//        result = target.exists();
//        assertTrue(result);
//        
//        target = CachedFile.create("/foo/data/1");
//        result = target.isFile();
//        assertFalse(result);
//        result = target.isDirectory();
//        assertTrue(result);
//        result = target.exists();
//        assertTrue(result);
//        
//        target = CachedFile.create("/foo/data/1/");
//        result = target.exists();
//        assertTrue(result);
//        result = target.isDirectory();
//        assertTrue(result);
//        result = target.isFile();
//        assertFalse(result);
//        
//        target = CachedFile.create("/foo/data/1/2/3/4/5/");
//        result = target.isDirectory();
//        assertTrue(result);
//        result = target.isFile();
//        assertFalse(result);
//        result = target.exists();
//        assertTrue(result);
//        
//        target = CachedFile.create("/foo/data/1/2/3/4/5/12345.file");
//        result = target.isDirectory();
//        assertFalse(result);
//        result = target.isFile();
//        assertTrue(result);
//        result = target.exists();
//        assertTrue(result);
//
//        target = CachedFile.create("/foo/data/1/2/3/4/1234.file");
//        result = target.exists();
//        assertTrue(result);
//        result = target.isDirectory();
//        assertFalse(result);
//        result = target.isFile();
//        assertTrue(result);
//        
//        target = CachedFile.create("/foo/data/3/4/5/345.file");
//        result = target.isDirectory();
//        assertFalse(result);
//        result = target.isFile();
//        assertTrue(result);
//        result = target.exists();
//        assertTrue(result);
//        
//        target = CachedFile.create("/foo/data/4/5/45.file");
//        result = target.isFile();
//        assertTrue(result);
//        result = target.isDirectory();
//        assertFalse(result);
//        result = target.exists();
//        assertTrue(result);
//        
//        
//        target = CachedFile.create("/foo/data/6");
//        result = target.isFile();
//        assertFalse(result);
//        result = target.isDirectory();
//        assertFalse(result);
//        result = target.exists();
//        assertFalse(result);
//    }
//
//    @Test
//    public void testLoadFile() throws IOException {
//        File result = null;
//        CachedFile target;
//        boolean sawException;
//        
//        sawException = false;
//        result = null;
//        try {
//            target = CachedFile.create("/foo/data/1/2/3");
//            result = target.loadFile();
//        } catch (IOException e) {
//            sawException = true;
//        }
//        assertNull(result);
//        assertTrue(sawException);
//
//        sawException = false;
//        result = null;
//        try {
//            target = CachedFile.create("/foo/data/1");
//            result = target.loadFile();
//        } catch (IOException e) {
//            sawException = true;
//        }
//        assertNull(result);
//        assertTrue(sawException);
//
//        sawException = false;
//        result = null;
//        try {
//            target = CachedFile.create("/foo/data/1/");
//            result = target.loadFile();
//        } catch (IOException e) {
//            sawException = true;
//        }
//        assertNull(result);
//        assertTrue(sawException);
//
//        sawException = false;
//        result = null;
//        try {
//            target = CachedFile.create("/foo/data/1/2/3/4/5/12345.file");
//            result = target.loadFile();
//        } catch (IOException e) {
//            sawException = true;
//        }
//        assertNotNull(result);
//        assertFalse(sawException);
//
//        sawException = false;
//        result = null;
//        try {
//            target = CachedFile.create("/foo/data/1/2/3/4/1234.file");
//            result = target.loadFile();
//        } catch (IOException e) {
//            sawException = true;
//        }
//        assertNotNull(result);
//        assertFalse(sawException);
//
//        sawException = false;
//        result = null;
//        try {
//            target = CachedFile.create("/foo/data/3/4/5/345.file");
//            result = target.loadFile();
//        } catch (IOException e) {
//            sawException = true;
//        }
//        assertNotNull(result);
//        assertFalse(sawException);
//
//        sawException = false;
//        result = null;
//        try {
//            target = CachedFile.create("/foo/data/4/5/45.file");
//            result = target.loadFile();
//        } catch (IOException e) {
//            sawException = true;
//        }
//        assertNotNull(result);
//        assertFalse(sawException);
//    }
//    
//    @Test
//    public void testListDirectory() throws IOException {
//        String[] result;
//        CachedFile target;
//        String[] dir = {"1", "2", "3", "4", "5"};
//        String[] dir123 = {"4", "123.file"};
//        String[] dir1 = {"1", "2", "1.file"};
//        String[] dir11 = new String[2502];
//        
//        for (int i = 0; i < 2500; i++) {
//            dir11[i] = Integer.toString(i) + ".file";
//        }
//        dir11[2500] = "2";
//        dir11[2501] = "3";
//        
//        Arrays.sort(dir123);
//        Arrays.sort(dir1);
//        Arrays.sort(dir11);
//        
//        target = CachedFile.create("/foo/data");
//        result = target.list();
//        assertNotNull(result);
//        assertArrayEquals(dir, result);
//        target = CachedFile.create("/foo/data/1/2/3");
//        result = target.list();
//        assertNotNull(result);
//        assertArrayEquals(dir123, result);
//        target = CachedFile.create("/foo/data/1/2/3/");
//        result = target.list();
//        assertNotNull(result);
//        assertArrayEquals(dir123, result);
//        target = CachedFile.create("/foo/data/1");
//        result = target.list();
//        assertNotNull(result);
//        Arrays.sort(result);
//        assertArrayEquals(dir1, result);
//        target = CachedFile.create("/foo/data/1/");
//        result = target.list();
//        assertNotNull(result);
//        Arrays.sort(result);
//        assertArrayEquals(dir1, result);
//
//        target = CachedFile.create("/foo/data/1/2/3/4/5/12345.file");
//        result = target.list();
//        assertNull(result);
//        target = CachedFile.create("/foo/data/1/2/3/4/1234.file");
//        result = target.list();
//        assertNull(result);
//        target = CachedFile.create("/foo/data/3/4/5/345.file");
//        result = target.list();
//        assertNull(result);
//        target = CachedFile.create("/foo/data/4/5/45.file");
//        result = target.list();
//        assertNull(result);
//        target = CachedFile.create("/foo/data/6");
//        result = target.list();
//        assertNull(result);
//
//        target = CachedFile.create("/foo/data/1/1/");
//        result = target.list();
//        assertNotNull(result);
//        Arrays.sort(result);
//        assertArrayEquals(dir11, result);
//        
//    }
//
//    @Test
//    public void testCacheRemove() throws IOException {
////        client.putObject(s3bucket, "1/1/2/112.file", dataFile);
////        client.putObject(s3bucket, "1/1/3/113.file", dataFile);
//    }
//    
//}
