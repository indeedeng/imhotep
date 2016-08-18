package com.indeed.imhotep.service;

import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.flamdex.writer.FlamdexDocument;
import com.indeed.imhotep.FTGSIteratorTestUtils;
import com.indeed.imhotep.FTGSIteratorUtil;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.client.ImhotepClient;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

/**
 * @author kenh
 */

public class TestImhotepGetFTGSIterator {
    private static final Logger LOGGER = Logger.getLogger(TestImhotepGetFTGSIterator.class);
    private static final DateTime TODAY = DateTime.now().withTimeAtStartOfDay();

    private static final String DATASET = "dataset";

    @Rule
    public TemporaryFolder rootDir = new TemporaryFolder();

    private ImhotepDaemonClusterRunner clusterRunner;

    @Before
    public void setUp() throws IOException {
        clusterRunner = new ImhotepDaemonClusterRunner(rootDir.getRoot());
    }

    @After
    public void tearDown() throws IOException, TimeoutException {
        clusterRunner.stop();
    }

    @Test
    public void testSingleSession() throws IOException, ImhotepOutOfMemoryException, InterruptedException, TimeoutException {
        final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 1)
                .addIntTerm("metric2", -1)
                .addIntTerms("if1", 1, 2)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "1a")
                .addStringTerms("sf2", "a")
                .build());

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 2)
                .addIntTerm("metric2", -2)
                .addIntTerms("if1", 21, 22)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "2a")
                .addStringTerms("sf2", "a")
                .build());

        memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                .addIntTerm("metric", 3)
                .addIntTerm("metric2", -3)
                .addIntTerms("if1", 31, 32)
                .addIntTerms("if2", 0)
                .addStringTerms("sf1", "3a")
                .addStringTerms("sf2", "a")
                .build());

        clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1), memoryFlamdex);
        clusterRunner.startDaemon(rootDir.getRoot().toPath());

        final ImhotepClient client = clusterRunner.createClient();

        final ImhotepSession dataset = client.sessionBuilder(DATASET, TODAY.minusDays(1), TODAY).build();

        // get full FTGS
        {
            final FTGSIterator iter = dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"});

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get first 12 terms
        {
            final FTGSIterator iter = dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 12);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        dataset.pushStat("metric");

        // get full FTGS with pushed stats (100 terms is enough to capture all)
        for (final FTGSIterator iter: Arrays.asList(
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
        ))
        {
            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get top 2 terms per field for the only group stat
        {
            final FTGSIterator iter = dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 0);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        dataset.pushStat("metric2");

        // get full FTGS with pushed stats (100 terms is enough to capture all)
        for (final FTGSIterator iter: Arrays.asList(
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
        ))
        {
            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get top 2 terms per field for the second group stat
        {
            final FTGSIterator iter = dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // map all documents with 'metric = 3' to group 2
        dataset.regroup(new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{ new RegroupCondition("metric", true, 3, null, false)})
        });

        // get full FTGS with pushed stats (100 terms is enough to capture all)
        for (final FTGSIterator iter: Arrays.asList(
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
        ))
        {
            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get top 2 terms per field, per group for the second group stat
        {
            final FTGSIterator iter = dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 3);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectEnd(iter);
        }
    }

    @Test
    public void testMultiSession() throws IOException, ImhotepOutOfMemoryException, InterruptedException, TimeoutException {

        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();

            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addIntTerm("metric", 1)
                    .addIntTerm("metric2", -1)
                    .addIntTerms("if1", 1, 2)
                    .addIntTerms("if2", 0)
                    .addStringTerms("sf1", "1a")
                    .addStringTerms("sf2", "a")
                    .build());

            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addIntTerm("metric", 2)
                    .addIntTerm("metric2", -2)
                    .addIntTerms("if1", 21, 22)
                    .addIntTerms("if2", 0)
                    .addStringTerms("sf1", "2a")
                    .addStringTerms("sf2", "a")
                    .build());

            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addIntTerm("metric", 3)
                    .addIntTerm("metric2", -3)
                    .addIntTerms("if1", 31, 32)
                    .addIntTerms("if2", 0)
                    .addStringTerms("sf1", "3a")
                    .addStringTerms("sf2", "a")
                    .build());

            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(2), memoryFlamdex);
        }

        {
            final MemoryFlamdex memoryFlamdex = new MemoryFlamdex();

            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addIntTerm("metric", 4)
                    .addIntTerm("metric2", -4)
                    .addIntTerms("if1", 41, 42)
                    .addIntTerms("if2", 0)
                    .addStringTerms("sf1", "4a")
                    .addStringTerms("sf2", "a")
                    .build());

            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addIntTerm("metric", 5)
                    .addIntTerm("metric2", -5)
                    .addIntTerms("if1", 51, 52)
                    .addIntTerms("if2", 0)
                    .addStringTerms("sf1", "5a")
                    .addStringTerms("sf2", "a")
                    .build());

            memoryFlamdex.addDocument(new FlamdexDocument.Builder()
                    .addIntTerm("metric", 6)
                    .addIntTerm("metric2", -6)
                    .addIntTerms("if1", 61, 62)
                    .addIntTerms("if2", 0)
                    .addStringTerms("sf1", "6a")
                    .addStringTerms("sf2", "a")
                    .build());

            clusterRunner.createDailyShard(DATASET, TODAY.minusDays(1), memoryFlamdex);
        }

        clusterRunner.startDaemon(rootDir.getRoot().toPath());
        clusterRunner.startDaemon(rootDir.getRoot().toPath());

        final ImhotepClient client = clusterRunner.createClient();

        final ImhotepSession dataset = client.sessionBuilder(DATASET, TODAY.minusDays(2), TODAY).build();

        // get first 15 terms
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 15),
                    1, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrField(iter, "sf2");

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get full FTGS
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}), 1, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});
            FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{0});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        dataset.pushStat("metric");

        // get full FTGS with pushed stats (100 terms is enough to capture all)
        for (final FTGSIterator iterator: Arrays.asList(
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
        ))
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    iterator, 1, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});
            FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
            FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get top 4 terms per field for the only group stat
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 4, 0), 1, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});
            FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3});
            FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4});
            FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5});
            FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        dataset.pushStat("metric2");

        // get full FTGS with pushed stats (100 terms is enough to capture all)
        for (final FTGSIterator iterator: Arrays.asList(
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
        ))
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    iterator, 2, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});
            FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{3, -3});
            FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get top 2 terms per field for the second group stat
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 2, 1), 2, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{21, -21});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // map all documents with 'metric = 3' to group 2
        dataset.regroup(new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 1, new int[]{2}, new RegroupCondition[]{ new RegroupCondition("metric", true, 3, null, false)}),
        });

        // get full FTGS with pushed stats (100 terms is enough to capture all)
        for (final FTGSIterator iterator: Arrays.asList(
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100),
                dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 100, 0)
        ))
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    iterator, 2, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 41, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 42, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 51, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 52, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 61, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});
            FTGSIteratorTestUtils.expectIntTerm(iter, 62, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectIntTerm(iter, 6, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});
            FTGSIteratorTestUtils.expectStrTerm(iter, "6a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{6, -6});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectEnd(iter);
        }

        // get top 4 terms per field, per group for the second group stat
        {
            final RawFTGSIterator iter = FTGSIteratorUtil.persist(LOGGER,
                    dataset.getFTGSIterator(new String[]{"if1", "if2", "metric"}, new String[]{"sf1", "sf2"}, 4, 1), 2, null);

            FTGSIteratorTestUtils.expectIntField(iter, "if1");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 21, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 22, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 31, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 32, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "if2");
            FTGSIteratorTestUtils.expectIntTerm(iter, 0, 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectIntField(iter, "metric");
            FTGSIteratorTestUtils.expectIntTerm(iter, 1, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectIntTerm(iter, 2, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectIntTerm(iter, 3, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectIntTerm(iter, 4, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectIntTerm(iter, 5, 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});

            FTGSIteratorTestUtils.expectStrField(iter, "sf1");
            FTGSIteratorTestUtils.expectStrTerm(iter, "1a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{1, -1});
            FTGSIteratorTestUtils.expectStrTerm(iter, "2a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{2, -2});
            FTGSIteratorTestUtils.expectStrTerm(iter, "3a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});
            FTGSIteratorTestUtils.expectStrTerm(iter, "4a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{4, -4});
            FTGSIteratorTestUtils.expectStrTerm(iter, "5a", 1);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{5, -5});

            FTGSIteratorTestUtils.expectStrField(iter, "sf2");
            FTGSIteratorTestUtils.expectStrTerm(iter, "a", 6);
            FTGSIteratorTestUtils.expectGroup(iter, 1, new long[]{18, -18});
            FTGSIteratorTestUtils.expectGroup(iter, 2, new long[]{3, -3});

            FTGSIteratorTestUtils.expectEnd(iter);
        }
    }
}
