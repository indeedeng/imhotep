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
 package com.indeed.imhotep.archive;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.indeed.imhotep.archive.CopyDirectoryToLocal;
import com.indeed.imhotep.archive.CopyFromLocal;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author jsgroth
 */
public class TestCopyDirectoryToLocal extends TestCase {
    private FileSystem fs;

    private String preFrom;
    private String from;
    private String to;

    @Override
    protected void setUp() throws Exception {
        fs = new NicerLocalFileSystem();
        preFrom = Files.createTempDir().getAbsolutePath();
        from = Files.createTempDir().getAbsolutePath();
        to = File.createTempFile("sqar-test", "").getAbsolutePath();
        new File(to).delete();
    }

    @Override
    protected void tearDown() throws Exception {
        fs.delete(new Path(from), true);
        fs.delete(new Path(to), true);
    }

    @Test
    public void testCopy() throws IOException {
        final List<String> dirs = Lists.newArrayList();
        final List<String> rawDirs = Lists.newArrayList();
        Random rand = new Random();
        byte[] bytes = new byte[1000];
        for (int i = 0; i < 5; ++i) {
            dirs.add(UUID.randomUUID().toString());
            rawDirs.add(dirs.get(dirs.size() - 1));
            File dir = new File(preFrom, dirs.get(dirs.size() - 1));
            dir.mkdir();
            for (int j = 0; j < 5; ++j) {
                OutputStream os = new FileOutputStream(new File(dir, Integer.toString(j)));
                rand.nextBytes(bytes);
                os.write(bytes);
                os.close();
            }
        }

        for (int i = 0; i < 5; ++i) {
            dirs.add(UUID.randomUUID().toString());
            rawDirs.add(dirs.get(dirs.size() - 1) + ".sqar");
            File dir = new File(preFrom, rawDirs.get(rawDirs.size() - 1));
            dir.mkdir();
            for (int j = 0; j < 5; ++j) {
                OutputStream os = new FileOutputStream(new File(dir, Integer.toString(j)));
                rand.nextBytes(bytes);
                os.write(bytes);
                os.close();
            }
        }

        for (File file : new File(preFrom).listFiles()) {
            CopyFromLocal.copy(fs, file, new Path(from, file.getName()), false);
        }
        CopyDirectoryToLocal.copy(fs, new Path(from), new File(to));

        for (int i = 0; i < dirs.size(); ++i) {
            File d1 = new File(preFrom, rawDirs.get(i));
            File d2 = new File(to, dirs.get(i));
            for (int j = 0; j < 5; ++j) {
                assertTrue(Files.equal(new File(d1, Integer.toString(j)), new File(d2, Integer.toString(j))));
            }
        }
    }
}
