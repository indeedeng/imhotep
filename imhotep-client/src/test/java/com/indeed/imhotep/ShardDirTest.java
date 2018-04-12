/*
 * Copyright (C) 2018 Indeed Inc.
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