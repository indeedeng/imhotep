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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author jsgroth
 */
public class TestCopyStuff {
    private FileSystem fs;

    private File temp1;
    private Path temp2;
    private File temp3;
    private File temp4;

    @Before
    public void setUp() throws IOException{
        fs = new NicerLocalFileSystem();

        temp1 = File.createTempFile("sqar-test", "");
        temp2 = fileToPath(Files.createTempDir());
        temp3 = Files.createTempDir();
        temp4 = Files.createTempDir();
    }

    @After
    public void tearDown() throws IOException {
        fs.delete(fileToPath(temp1), true);
        fs.delete(temp2, true);
        fs.delete(fileToPath(temp3), true);
        fs.delete(fileToPath(temp4), true);
    }

    @Test
    public void testSingleFile() throws IOException {
        clearTemps();

        final String temp1Filename = temp1.getName();
        try {
            CopyFromLocal.copy(fs, temp1, temp2, false);
            fail();
        } catch (final IOException e) {
            // pass
        }
        CopyFromLocal.copy(fs, temp1, temp2, true);
        CopyToLocal.copy(fs, temp2, temp3);
        
        assertTrue(new File(temp3, temp1Filename).exists());
        assertTrue(Files.equal(temp1, new File(temp3, temp1Filename)));
    }

    private void clearTemps() throws IOException {
        fs.delete(temp2, true);
        temp2 = fileToPath(Files.createTempDir());
        fs.delete(fileToPath(temp3), true);
        temp3 = Files.createTempDir();
    }

    @Test
    public void testDirectory() throws IOException {
        clearTemps();

        final List<String> files = Lists.newArrayList();
        final byte[] bytes = new byte[1000];
        final Random rand = new Random();
        for (int i = 0; i < 10; ++i) {
            files.add(UUID.randomUUID().toString());
            try( final OutputStream os = new FileOutputStream(new File(temp4, files.get(i))) ) {
                rand.nextBytes(bytes);
                os.write(bytes);
            }
        }
        final List<String> dirs = Lists.newArrayList();
        for (int i = 0; i < 10; ++i) {
            dirs.add(UUID.randomUUID().toString());
            final File dir = new File(temp4, dirs.get(i));
            if (!dir.mkdir()) {
                throw new IOException();
            }
            try( final OutputStream os = new FileOutputStream(new File(dir, "asdf")) ) {
                rand.nextBytes(bytes);
                os.write(bytes);
            }
        }

        CopyFromLocal.copy(fs, temp4, temp2, true);
        CopyToLocal.copy(fs, temp2, temp3);

        for (final String file : files) {
            assertTrue(Files.equal(new File(temp4, file), new File(temp3.toString(), file)));
        }

        for (final String dir : dirs) {
            final File orig = new File(new File(temp4, dir), "asdf");
            final File copied = new File(new File(temp3.toString(), dir), "asdf");
            assertTrue(Files.equal(orig, copied));
        }
    }

    private static Path fileToPath(final File f) {
        return new Path(f.getAbsolutePath());
    }
}
