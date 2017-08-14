package com.indeed.imhotep;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

/**
 * @author michihiko
 */
public class ShardDirTest {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testNormalShard() {
        final Path tempPath = tempFolder.getRoot().toPath();
        final ShardDir shardDir = new ShardDir(tempPath.resolve("index19700101.00-19700101.01.20161231235959"));
        assertEquals("index19700101.00-19700101.01.20161231235959", shardDir.getName());
        assertEquals("index19700101.00-19700101.01", shardDir.getId());
        assertEquals(20161231235959L, shardDir.getVersion());
        assertEquals(tempPath.resolve("index19700101.00-19700101.01.20161231235959"), shardDir.getIndexDir());
    }

    @Test
    public void testDynamicShard() {
        final Path tempPath = tempFolder.getRoot().toPath();
        final ShardDir shardDir = new ShardDir(tempPath.resolve("dindex19700101.00-19700101.01.1.4.1234.5678"));
        assertEquals("dindex19700101.00-19700101.01.1.4.1234.5678", shardDir.getName());
        assertEquals("dindex19700101.00-19700101.01.1.4", shardDir.getId());
        assertEquals(1234L, shardDir.getVersion());
        assertEquals(tempPath.resolve("dindex19700101.00-19700101.01.1.4.1234.5678"), shardDir.getIndexDir());
    }
}