package com.indeed.imhotep.io.caching;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for S3CachedFileSystem
 */
public class SqarTest {
    private static List<Map<String,Object>> testSettings;
    
    @BeforeClass
    public static void init() throws IOException {
        
        testSettings = new ArrayList<Map<String,Object>>();
        
        Map<String,Object> s3layer = new HashMap<String,Object>();
        s3layer.put("type", "SQAR_AUTOMOUNTING");
        s3layer.put("order", 2);
        s3layer.put("mountpoint", "/");
        testSettings.add(s3layer);
        
        Map<String,Object> cachelayer = new HashMap<String,Object>();
        cachelayer.put("type", "CACHED");
        cachelayer.put("order", 3);
        cachelayer.put("mountpoint", "/");
        cachelayer.put("cache-dir", "/tmp/sqar_test_temp_cache");
        cachelayer.put("cacheSizeMB", 500);
        testSettings.add(cachelayer);
        
        CachedFile.init(testSettings, "testData/", false);
        
    }

    @AfterClass
    public static void cleanup() throws IOException {
        
        CachedFile.init(null, "/", true);
        
    }

    
    @Test
    public void testSqarExists() throws IOException {
        boolean result;
        CachedFile target;
        
        target = CachedFile.create("testData/test-archive/");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        
        target = CachedFile.create("testData/test-archive");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        
        target = CachedFile.create("testData/test-archive/1/2/3");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        
        target = CachedFile.create("testData/test-archive/1/2/3/");
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/test-archive/1");
        result = target.isFile();
        assertFalse(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/test-archive/1/");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        
        target = CachedFile.create("testData/test-archive/1/2/3/4/5/");
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/test-archive/1/2/3/4/5/12345.file");
        result = target.isDirectory();
        assertFalse(result);
        result = target.isFile();
        assertTrue(result);
        result = target.exists();
        assertTrue(result);

        target = CachedFile.create("testData/test-archive/1/2/3/4/1234.file");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertFalse(result);
        result = target.isFile();
        assertTrue(result);
        
        target = CachedFile.create("testData/test-archive/3/4/5/345.file");
        result = target.isDirectory();
        assertFalse(result);
        result = target.isFile();
        assertTrue(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/test-archive/4/5/45.file");
        result = target.isFile();
        assertTrue(result);
        result = target.isDirectory();
        assertFalse(result);
        result = target.exists();
        assertTrue(result);
        
        
        target = CachedFile.create("testData/test-archive/6");
        result = target.isFile();
        assertFalse(result);
        result = target.isDirectory();
        assertFalse(result);
        result = target.exists();
        assertFalse(result);
    }

    @Test
    public void testSqarListDirectory() throws IOException {
        String[] result;
        CachedFile target;
        String[] dir0 = {"1", "2", "3", "4", "5", "test-archive"};
        String[] dir123 = {"4", "123.file"};
        String[] dir1 = {"1", "2", "1.file"};
        String[] dir11 = new String[2502];
        
        for (int i = 0; i < 2500; i++) {
            dir11[i] = Integer.toString(i) + ".file";
        }
        dir11[2500] = "2";
        dir11[2501] = "3";
        
        Arrays.sort(dir123);
        Arrays.sort(dir1);
        Arrays.sort(dir11);
        
        target = CachedFile.create("testData");
        result = target.list();
        Arrays.sort(result);
        assertNotNull(result);
        assertArrayEquals(dir0, result);

        target = CachedFile.create("testData/test-archive/1/2/3");
        result = target.list();
        Arrays.sort(result);
        assertNotNull(result);
        assertArrayEquals(dir123, result);
        target = CachedFile.create("testData/test-archive/1/2/3/");
        result = target.list();
        Arrays.sort(result);
        assertNotNull(result);
        assertArrayEquals(dir123, result);
        target = CachedFile.create("testData/test-archive/1");
        result = target.list();
        assertNotNull(result);
        Arrays.sort(result);
        assertArrayEquals(dir1, result);
        target = CachedFile.create("testData/test-archive/1/");
        result = target.list();
        assertNotNull(result);
        Arrays.sort(result);
        assertArrayEquals(dir1, result);

        target = CachedFile.create("testData/test-archive/1/2/3/4/5/12345.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/test-archive/1/2/3/4/1234.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/test-archive/3/4/5/345.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/test-archive/4/5/45.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/test-archive/6");
        result = target.list();
        assertNull(result);

        target = CachedFile.create("testData/test-archive/1/1/");
        result = target.list();
        assertNotNull(result);
        Arrays.sort(result);
        assertArrayEquals(dir11, result);
        
    }

    @Test
    public void testSqarLoadFile() throws IOException {
        File result = null;
        CachedFile target;
        boolean sawException;
        
        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/test-archive/1/2/3");
            result = target.loadFile();
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/test-archive/1");
            result = target.loadFile();
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/test-archive/1/");
            result = target.loadFile();
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/test-archive/1/2/3/4/5/12345.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 12345; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            sawException = true;
        }
        assertNotNull(result);
        assertFalse(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/test-archive/1/2/3/4/1234.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 1234; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            sawException = true;
        }
        assertNotNull(result);
        assertFalse(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/test-archive/3/4/5/345.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 345; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            sawException = true;
        }
        assertNotNull(result);
        assertFalse(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/test-archive/4/5/45.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 45; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
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
    public void testSqarLoadDirectory() throws IOException {
        File result;
        boolean bresult;
        CachedFile target;
        
        target = CachedFile.create("testData/test-archive/1/2/3");
        bresult = target.exists();
        assertTrue(bresult);
        result = target.loadDirectory();
        assertNotNull(result);
    }
    
    @Test
    public void testExists() throws IOException {
        boolean result;
        CachedFile target;
        
        target = CachedFile.create("testData/1/2/3");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        
        target = CachedFile.create("testData/1/2/3/");
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/1");
        result = target.isFile();
        assertFalse(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/1/");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        
        target = CachedFile.create("testData/1/2/3/4/5/");
        result = target.isDirectory();
        assertTrue(result);
        result = target.isFile();
        assertFalse(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/1/2/3/4/5/12345.file");
        result = target.isDirectory();
        assertFalse(result);
        result = target.isFile();
        assertTrue(result);
        result = target.exists();
        assertTrue(result);

        target = CachedFile.create("testData/1/2/3/4/1234.file");
        result = target.exists();
        assertTrue(result);
        result = target.isDirectory();
        assertFalse(result);
        result = target.isFile();
        assertTrue(result);
        
        target = CachedFile.create("testData/3/4/5/345.file");
        result = target.isDirectory();
        assertFalse(result);
        result = target.isFile();
        assertTrue(result);
        result = target.exists();
        assertTrue(result);
        
        target = CachedFile.create("testData/4/5/45.file");
        result = target.isFile();
        assertTrue(result);
        result = target.isDirectory();
        assertFalse(result);
        result = target.exists();
        assertTrue(result);
        
        
        target = CachedFile.create("testData/6");
        result = target.isFile();
        assertFalse(result);
        result = target.isDirectory();
        assertFalse(result);
        result = target.exists();
        assertFalse(result);
    }

    @Test
    public void testLoadFile() throws IOException {
        File result = null;
        CachedFile target;
        boolean sawException;
        
        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/1/2/3");
            result = target.loadFile();
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/1");
            result = target.loadFile();
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/1/");
            result = target.loadFile();
        } catch (IOException e) {
            sawException = true;
        }
        assertNull(result);
        assertTrue(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/1/2/3/4/5/12345.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 12345; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            sawException = true;
        }
        assertNotNull(result);
        assertFalse(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/1/2/3/4/1234.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 1234; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            sawException = true;
        }
        assertNotNull(result);
        assertFalse(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/3/4/5/345.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 345; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
            } finally {
                fis.close();
            }
        } catch (IOException e) {
            sawException = true;
        }
        assertNotNull(result);
        assertFalse(sawException);

        sawException = false;
        result = null;
        try {
            target = CachedFile.create("testData/4/5/45.file");
            result = target.loadFile();
            
            int data;
            FileReader fis = null;
            String expected = "";
            try {
                for (int i = 0; i< 45; i ++) {
                    expected += "foo!";
                }
                
                fis = new FileReader(result);
                for (int i = 0; i < expected.length(); i++) {
                    data = fis.read();
                    assert(data == expected.charAt(i));
                }
                assert(fis.read() == -1);
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
    public void testListDirectory() throws IOException {
        String[] result;
        CachedFile target;
        String[] dir123 = {"4", "123.file"};
        String[] dir1 = {"1", "2", "1.file"};
        String[] dir11 = new String[2502];
        
        for (int i = 0; i < 2500; i++) {
            dir11[i] = Integer.toString(i) + ".file";
        }
        dir11[2500] = "2";
        dir11[2501] = "3";
        
        Arrays.sort(dir123);
        Arrays.sort(dir1);
        Arrays.sort(dir11);
        
        target = CachedFile.create("testData/1/2/3");
        result = target.list();
        Arrays.sort(result);
        assertNotNull(result);
        assertArrayEquals(dir123, result);
        target = CachedFile.create("testData/1/2/3/");
        result = target.list();
        Arrays.sort(result);
        assertNotNull(result);
        assertArrayEquals(dir123, result);
        target = CachedFile.create("testData/1");
        result = target.list();
        assertNotNull(result);
        Arrays.sort(result);
        assertArrayEquals(dir1, result);
        target = CachedFile.create("testData/1/");
        result = target.list();
        assertNotNull(result);
        Arrays.sort(result);
        assertArrayEquals(dir1, result);

        target = CachedFile.create("testData/1/2/3/4/5/12345.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/1/2/3/4/1234.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/3/4/5/345.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/4/5/45.file");
        result = target.list();
        assertNull(result);
        target = CachedFile.create("testData/6");
        result = target.list();
        assertNull(result);

        target = CachedFile.create("testData/1/1/");
        result = target.list();
        assertNotNull(result);
        Arrays.sort(result);
        assertArrayEquals(dir11, result);
        
    }

    
    @Test
    public void testListFiles() throws IOException {
        CachedFile[] result;
        String[] fnames;
        CachedFile target;
        String[] dir123 = {"testData/1/2/3/4", "testData/1/2/3/123.file"};
        String[] dir1 = {"testData/1/1", "testData/1/2", "testData/1/1.file"};
        String[] dir11 = new String[2502];
        
        for (int i = 0; i < 2500; i++) {
            dir11[i] = "testData/1/1/" + Integer.toString(i) + ".file";
        }
        dir11[2500] = "testData/1/1/2";
        dir11[2501] = "testData/1/1/3";
        
        Arrays.sort(dir123);
        Arrays.sort(dir1);
        Arrays.sort(dir11);
        
        target = CachedFile.create("testData/1/2/3");
        result = target.listFiles();
        fnames = new String[result.length];
        for (int i = 0; i < result.length; i++) {
            CachedFile f = result[i];
            fnames[i] = f.getCanonicalPath();
        }
        Arrays.sort(fnames);
        assertNotNull(result);
        assertArrayEquals(dir123, fnames);
        target = CachedFile.create("testData/1/2/3/");
        result = target.listFiles();
        fnames = new String[result.length];
        for (int i = 0; i < result.length; i++) {
            CachedFile f = result[i];
            fnames[i] = f.getCanonicalPath();
        }
        Arrays.sort(fnames);
        assertNotNull(result);
        assertArrayEquals(dir123, fnames);
        target = CachedFile.create("testData/1");
        result = target.listFiles();
        assertNotNull(result);
        fnames = new String[result.length];
        for (int i = 0; i < result.length; i++) {
            CachedFile f = result[i];
            fnames[i] = f.getCanonicalPath();
        }
        Arrays.sort(fnames);
        assertArrayEquals(dir1, fnames);
        target = CachedFile.create("testData/1/");
        result = target.listFiles();
        assertNotNull(result);
        fnames = new String[result.length];
        for (int i = 0; i < result.length; i++) {
            CachedFile f = result[i];
            fnames[i] = f.getCanonicalPath();
        }
        Arrays.sort(fnames);
        assertArrayEquals(dir1, fnames);

        target = CachedFile.create("testData/1/2/3/4/5/12345.file");
        result = target.listFiles();
        assertNull(result);
        target = CachedFile.create("testData/1/2/3/4/1234.file");
        result = target.listFiles();
        assertNull(result);
        target = CachedFile.create("testData/3/4/5/345.file");
        result = target.listFiles();
        assertNull(result);
        target = CachedFile.create("testData/4/5/45.file");
        result = target.listFiles();
        assertNull(result);
        target = CachedFile.create("testData/6");
        result = target.listFiles();
        assertNull(result);

        target = CachedFile.create("testData/1/1/");
        result = target.listFiles();
        assertNotNull(result);
        fnames = new String[result.length];
        for (int i = 0; i < result.length; i++) {
            CachedFile f = result[i];
            fnames[i] = f.getCanonicalPath();
        }
        Arrays.sort(fnames);
        assertArrayEquals(dir11, fnames);
        
    }

    @Test
    public void testCacheRemove() throws IOException {
//        client.putObject(s3bucket, "1/1/2/112.file", dataFile);
//        client.putObject(s3bucket, "1/1/3/113.file", dataFile);
    }
    
}
