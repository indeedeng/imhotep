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

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import static com.indeed.imhotep.FTGSIteratorTestUtils.expectEnd;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectFieldEnd;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectGroup;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectIntField;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectIntTerm;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectStrField;
import static com.indeed.imhotep.FTGSIteratorTestUtils.expectStrTerm;

/**
 * @author kenh
 */

public class FTGSIteratorUtilTest {
    private static final Logger LOGGER = Logger.getLogger(FTGSIteratorUtilTest.class);

    @Rule
    public final ExpectedException expected = ExpectedException.none();
    private File file;

    @After
    public void tearDown() {
        if (file != null) {
            file.delete();
        }
    }

    @Test
    public void testIteratorPersist() throws IOException {
        final int numStats = 2;
        final int numGroups = 2;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try( final FTGSOutputStreamWriter w = new FTGSOutputStreamWriter(out) ) {

            w.switchField("a", true);

            w.switchIntTerm(1, 5);

            w.switchGroup(0);
            w.addStat(1000);
            w.addStat(20);

            w.switchGroup(1);
            w.addStat(100);
            w.addStat(200);

            w.switchIntTerm(2, 10);

            w.switchGroup(0);
            w.addStat(10);
            w.addStat(2000);

            w.switchGroup(1);
            w.addStat(200);
            w.addStat(-100);

            w.switchIntTerm(3, 15);

            w.switchGroup(0);
            w.addStat(100);
            w.addStat(200);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(400);

            w.switchIntTerm(4, 20);

            w.switchGroup(0);
            w.addStat(10000);
            w.addStat(20000);

            w.switchGroup(1);
            w.addStat(-300);
            w.addStat(500);
        }

        final FTGSIterator iter = FTGSIteratorUtil.persist(LOGGER, new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), numStats, numGroups));

        expectIntField(iter, "a");

        expectIntTerm(iter, 1, 5);
        expectGroup(iter, 0, new long[]{1000, 20});
        expectGroup(iter, 1, new long[]{100, 200});

        expectIntTerm(iter, 2, 10);
        expectGroup(iter, 0, new long[]{10, 2000});
        expectGroup(iter, 1, new long[]{200, -100});

        expectIntTerm(iter, 3, 15);
        expectGroup(iter, 0, new long[]{100, 200});
        expectGroup(iter, 1, new long[]{300, 400});

        expectIntTerm(iter, 4, 20);
        expectGroup(iter, 0, new long[]{10000, 20000});
        expectGroup(iter, 1, new long[]{-300, 500});

        expectFieldEnd(iter);
    }

    @Test
    public void testTopTermsByStats() throws IOException {
        final int numStats = 3;
        final int numGroups = 3;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try( final FTGSOutputStreamWriter w = new FTGSOutputStreamWriter(out) ) {

            w.switchField("a", true);

            w.switchIntTerm(1, 5);

            w.switchGroup(1);
            w.addStat(100);
            w.addStat(200);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(-300);
            w.addStat(500);
            w.addStat(0);

            w.switchIntTerm(2, 10);

            w.switchGroup(1);
            w.addStat(200);
            w.addStat(-100);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(300);
            w.addStat(400);
            w.addStat(0);

            w.switchIntTerm(3, 15);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(400);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(200);
            w.addStat(-100);
            w.addStat(0);

            w.switchIntTerm(4, 20);

            w.switchGroup(1);
            w.addStat(-300);
            w.addStat(500);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(100);
            w.addStat(200);
            w.addStat(0);

            w.switchField("b", true);

            w.switchIntTerm(11, 5);

            w.switchGroup(1);
            w.addStat(-100);
            w.addStat(-200);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(300);
            w.addStat(-500);
            w.addStat(0);

            w.switchIntTerm(12, 10);

            w.switchGroup(1);
            w.addStat(-200);
            w.addStat(100);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(-300);
            w.addStat(-400);
            w.addStat(0);

            w.switchIntTerm(13, 15);

            w.switchGroup(1);
            w.addStat(-300);
            w.addStat(-400);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(-200);
            w.addStat(100);
            w.addStat(0);

            w.switchIntTerm(14, 20);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(-500);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(-100);
            w.addStat(-200);
            w.addStat(0);

            w.switchField("b", false);

            w.switchBytesTerm("aA".getBytes(), "aA".getBytes().length, 5);

            w.switchGroup(1);
            w.addStat(100);
            w.addStat(-250);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(300);
            w.addStat(4000);
            w.addStat(0);

            w.switchBytesTerm("bb".getBytes(), "bb".getBytes().length, 10);

            w.switchGroup(1);
            w.addStat(200);
            w.addStat(1000);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(300);
            w.addStat(-500);
            w.addStat(0);

            w.switchBytesTerm("c".getBytes(), "c".getBytes().length, 15);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(4000);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(200);
            w.addStat(1000);
            w.addStat(0);

            w.switchBytesTerm("d".getBytes(), "d".getBytes().length, 20);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(-500);
            w.addStat(0);

            w.switchGroup(2);
            w.addStat(100);
            w.addStat(-250);
            w.addStat(0);
        }

        {
            final InputStreamFTGSIterator iter = new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), numStats, numGroups);

            expectIntField(iter, "a");

            expectIntTerm(iter, 1, 5);
            expectGroup(iter, 1, new long[]{100, 200, 0});
            expectGroup(iter, 2, new long[]{-300, 500, 0});

            expectIntTerm(iter, 2, 10);
            expectGroup(iter, 1, new long[]{200, -100, 0});
            expectGroup(iter, 2, new long[]{300, 400, 0});

            expectIntTerm(iter, 3, 15);
            expectGroup(iter, 1, new long[]{300, 400, 0});
            expectGroup(iter, 2, new long[]{200, -100, 0});

            expectIntTerm(iter, 4, 20);
            expectGroup(iter, 1, new long[]{-300, 500, 0});
            expectGroup(iter, 2, new long[]{100, 200, 0});

            expectFieldEnd(iter);

            expectIntField(iter, "b");

            expectIntTerm(iter, 11, 5);
            expectGroup(iter, 1, new long[]{-100, -200, 0});
            expectGroup(iter, 2, new long[]{300, -500, 0});

            expectIntTerm(iter, 12, 10);
            expectGroup(iter, 1, new long[]{-200, 100, 0});
            expectGroup(iter, 2, new long[]{-300, -400, 0});

            expectIntTerm(iter, 13, 15);
            expectGroup(iter, 1, new long[]{-300, -400, 0});
            expectGroup(iter, 2, new long[]{-200, 100, 0});

            expectIntTerm(iter, 14, 20);
            expectGroup(iter, 1, new long[]{300, -500, 0});
            expectGroup(iter, 2, new long[]{-100, -200, 0});

            expectFieldEnd(iter);

            expectStrField(iter, "b");

            expectStrTerm(iter, "aA", 5);
            expectGroup(iter, 1, new long[]{100, -250, 0});
            expectGroup(iter, 2, new long[]{300, 4000, 0});

            expectStrTerm(iter, "bb", 10);
            expectGroup(iter, 1, new long[]{200, 1000, 0});
            expectGroup(iter, 2, new long[]{300, -500, 0});

            expectStrTerm(iter, "c", 15);
            expectGroup(iter, 1, new long[]{300, 4000, 0});
            expectGroup(iter, 2, new long[]{200, 1000, 0});

            expectStrTerm(iter, "d", 20);
            expectGroup(iter, 1, new long[]{300, -500, 0});
            expectGroup(iter, 2, new long[]{100, -250, 0});

            expectFieldEnd(iter);

            expectEnd(iter);
        }

        file = FTGSIteratorUtil.persistAsFile(LOGGER, "test", new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), numStats, numGroups));

        {
            final TopTermsFTGSIterator iter = FTGSIteratorUtil.getTopTermsFTGSIterator(
                    InputStreamFTGSIterators.create(file, numStats, numGroups), 2, 0);

            expectIntField(iter, "a");

            expectIntTerm(iter, 2, 10);
            expectGroup(iter, 1, new long[]{200, -100, 0});
            expectGroup(iter, 2, new long[]{300, 400, 0});

            expectIntTerm(iter, 3, 15);
            expectGroup(iter, 1, new long[]{300, 400, 0});
            expectGroup(iter, 2, new long[]{200, -100, 0});

            expectFieldEnd(iter);

            expectIntField(iter, "b");

            expectIntTerm(iter, 11, 5);
            expectGroup(iter, 1, new long[]{-100, -200, 0});
            expectGroup(iter, 2, new long[]{300, -500, 0});

            expectIntTerm(iter, 14, 20);
            expectGroup(iter, 1, new long[]{300, -500, 0});
            expectGroup(iter, 2, new long[]{-100, -200, 0});

            expectFieldEnd(iter);

            expectStrField(iter, "b");

            expectStrTerm(iter, "aA", 5);
            expectGroup(iter, 2, new long[]{300, 4000, 0});

            expectStrTerm(iter, "bb", 10);
            expectGroup(iter, 2, new long[]{300, -500, 0});

            expectStrTerm(iter, "c", 15);
            expectGroup(iter, 1, new long[]{300, 4000, 0});

            expectStrTerm(iter, "d", 20);
            expectGroup(iter, 1, new long[]{300, -500, 0});

            expectFieldEnd(iter);

            expectEnd(iter);
        }

        {
            final TopTermsFTGSIterator iter = FTGSIteratorUtil.getTopTermsFTGSIterator(
                    InputStreamFTGSIterators.create(file, numStats, numGroups), 2, 1);

            expectIntField(iter, "a");

            expectIntTerm(iter, 1, 5);
            expectGroup(iter, 2, new long[]{-300, 500, 0});

            expectIntTerm(iter, 2, 10);
            expectGroup(iter, 2, new long[]{300, 400, 0});

            expectIntTerm(iter, 3, 15);
            expectGroup(iter, 1, new long[]{300, 400, 0});

            expectIntTerm(iter, 4, 20);
            expectGroup(iter, 1, new long[]{-300, 500, 0});

            expectFieldEnd(iter);

            expectIntField(iter, "b");

            expectIntTerm(iter, 11, 5);
            expectGroup(iter, 1, new long[]{-100, -200, 0});

            expectIntTerm(iter, 12, 10);
            expectGroup(iter, 1, new long[]{-200, 100, 0});

            expectIntTerm(iter, 13, 15);
            expectGroup(iter, 2, new long[]{-200, 100, 0});

            expectIntTerm(iter, 14, 20);
            expectGroup(iter, 2, new long[]{-100, -200, 0});

            expectFieldEnd(iter);

            expectStrField(iter, "b");

            expectStrTerm(iter, "aA", 5);
            expectGroup(iter, 2, new long[]{300, 4000, 0});

            expectStrTerm(iter, "bb", 10);
            expectGroup(iter, 1, new long[]{200, 1000, 0});

            expectStrTerm(iter, "c", 15);
            expectGroup(iter, 1, new long[]{300, 4000, 0});
            expectGroup(iter, 2, new long[]{200, 1000, 0});

            expectFieldEnd(iter);

            expectEnd(iter);
        }

        {
            final TopTermsFTGSIterator iter = FTGSIteratorUtil.getTopTermsFTGSIterator(
                    InputStreamFTGSIterators.create(file, numStats, numGroups), 2, 2);

            expectIntField(iter, "a");

            expectIntTerm(iter, 1, 5);
            expectGroup(iter, 1, new long[]{100, 200, 0});
            expectGroup(iter, 2, new long[]{-300, 500, 0});

            expectIntTerm(iter, 2, 10);
            expectGroup(iter, 1, new long[]{200, -100, 0});
            expectGroup(iter, 2, new long[]{300, 400, 0});

            expectFieldEnd(iter);

            expectIntField(iter, "b");

            expectIntTerm(iter, 11, 5);
            expectGroup(iter, 1, new long[]{-100, -200, 0});
            expectGroup(iter, 2, new long[]{300, -500, 0});

            expectIntTerm(iter, 12, 10);
            expectGroup(iter, 1, new long[]{-200, 100, 0});
            expectGroup(iter, 2, new long[]{-300, -400, 0});

            expectFieldEnd(iter);

            expectStrField(iter, "b");

            expectStrTerm(iter, "aA", 5);
            expectGroup(iter, 1, new long[]{100, -250, 0});
            expectGroup(iter, 2, new long[]{300, 4000, 0});

            expectStrTerm(iter, "bb", 10);
            expectGroup(iter, 1, new long[]{200, 1000, 0});
            expectGroup(iter, 2, new long[]{300, -500, 0});

            expectFieldEnd(iter);

            expectEnd(iter);
        }
    }

    @Test
    public void testTopTermsByStatsSingleGroup() throws IOException {
        final int numStats = 2;
        final int numGroups = 2;
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try( final FTGSOutputStreamWriter w = new FTGSOutputStreamWriter(out) ) {

            w.switchField("a", true);

            w.switchIntTerm(1, 5);

            w.switchGroup(1);
            w.addStat(100);
            w.addStat(200);

            w.switchIntTerm(2, 10);

            w.switchGroup(1);
            w.addStat(200);
            w.addStat(-100);

            w.switchIntTerm(3, 15);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(400);

            w.switchIntTerm(4, 20);

            w.switchGroup(1);
            w.addStat(-300);
            w.addStat(500);

            w.switchField("b", true);

            w.switchIntTerm(11, 5);

            w.switchGroup(1);
            w.addStat(-100);
            w.addStat(-200);

            w.switchIntTerm(12, 10);

            w.switchGroup(1);
            w.addStat(-200);
            w.addStat(100);

            w.switchIntTerm(13, 15);

            w.switchGroup(1);
            w.addStat(-300);
            w.addStat(-400);

            w.switchIntTerm(14, 20);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(-500);

            w.switchField("b", false);

            w.switchBytesTerm("aA".getBytes(), "aA".getBytes().length, 5);

            w.switchGroup(1);
            w.addStat(100);
            w.addStat(-250);

            w.switchBytesTerm("bb".getBytes(), "bb".getBytes().length, 10);

            w.switchGroup(1);
            w.addStat(200);
            w.addStat(1000);

            w.switchBytesTerm("c".getBytes(), "c".getBytes().length, 15);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(4000);

            w.switchBytesTerm("d".getBytes(), "d".getBytes().length, 20);

            w.switchGroup(1);
            w.addStat(300);
            w.addStat(-500);
        }

        {
            final InputStreamFTGSIterator iter = new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), numStats, numGroups);

            expectIntField(iter, "a");

            expectIntTerm(iter, 1, 5);
            expectGroup(iter, 1, new long[]{100, 200});

            expectIntTerm(iter, 2, 10);
            expectGroup(iter, 1, new long[]{200, -100});

            expectIntTerm(iter, 3, 15);
            expectGroup(iter, 1, new long[]{300, 400});

            expectIntTerm(iter, 4, 20);
            expectGroup(iter, 1, new long[]{-300, 500});

            expectFieldEnd(iter);

            expectIntField(iter, "b");

            expectIntTerm(iter, 11, 5);
            expectGroup(iter, 1, new long[]{-100, -200});

            expectIntTerm(iter, 12, 10);
            expectGroup(iter, 1, new long[]{-200, 100});

            expectIntTerm(iter, 13, 15);
            expectGroup(iter, 1, new long[]{-300, -400});

            expectIntTerm(iter, 14, 20);
            expectGroup(iter, 1, new long[]{300, -500});

            expectFieldEnd(iter);

            expectStrField(iter, "b");

            expectStrTerm(iter, "aA", 5);
            expectGroup(iter, 1, new long[]{100, -250});

            expectStrTerm(iter, "bb", 10);
            expectGroup(iter, 1, new long[]{200, 1000});

            expectStrTerm(iter, "c", 15);
            expectGroup(iter, 1, new long[]{300, 4000});

            expectStrTerm(iter, "d", 20);
            expectGroup(iter, 1, new long[]{300, -500});

            expectFieldEnd(iter);

            expectEnd(iter);
        }

        file = FTGSIteratorUtil.persistAsFile(LOGGER, "test", new InputStreamFTGSIterator(new ByteArrayInputStream(out.toByteArray()), numStats, numGroups));

        {
            final TopTermsFTGSIterator iter = FTGSIteratorUtil.getTopTermsFTGSIterator(
                    InputStreamFTGSIterators.create(file, numStats, numGroups), 2, 0);

            expectIntField(iter, "a");

            expectIntTerm(iter, 2, 10);
            expectGroup(iter, 1, new long[]{200, -100});

            expectIntTerm(iter, 3, 15);
            expectGroup(iter, 1, new long[]{300, 400});

            expectFieldEnd(iter);

            expectIntField(iter, "b");

            expectIntTerm(iter, 11, 5);
            expectGroup(iter, 1, new long[]{-100, -200});

            expectIntTerm(iter, 14, 20);
            expectGroup(iter, 1, new long[]{300, -500});

            expectFieldEnd(iter);

            expectStrField(iter, "b");

            expectStrTerm(iter, "c", 15);
            expectGroup(iter, 1, new long[]{300, 4000});

            expectStrTerm(iter, "d", 20);
            expectGroup(iter, 1, new long[]{300, -500});

            expectFieldEnd(iter);

            expectEnd(iter);
        }
   }
}