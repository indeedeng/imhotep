package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.CommandSerializationParameters;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.protobuf.Operator;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Validating deserialization of ImhotepCommand gives expected command and parameters.
 */
public class TestImhotepCommandDeserialization implements CommandsTest {

    private static final String SESSION_ID = "RandomSessionIdString";
    private static final String RANDOM_SALT = "RandomSaltString";
    private static final String TEST_INPUT_GROUPS_NAME = "myOtherTestGroups";
    private static final RegroupParams TEST_REGROUP_PARAMS = new RegroupParams("myInputGroups", "myOutputGroups");

    private void assertEqualDeserialize(final ImhotepCommand command) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        command.writeToOutputStream(outputStream, new CommandSerializationParameters() {
            @Override
            public String getHost() {
                return "localhost";
            }

            @Override
            public int getPort() {
                return 1337;
            }

            @Override
            public AtomicLong getTempFileSizeBytesLeft() {
                return new AtomicLong();
            }
        });
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ImhotepCommand deserializedCommand = ImhotepCommand.readFromInputStream(inputStream);
        Assert.assertEquals(0, inputStream.available());
        Assert.assertEquals(command, deserializedCommand);
    }

    @Override
    @Test
    public void testGetGroupStats() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new GetGroupStats(TEST_INPUT_GROUPS_NAME, stats, SESSION_ID));
    }

    @Override
    @Test
    public void testIntOrRegroup() throws IOException {
        assertEqualDeserialize(new IntOrRegroup(TEST_REGROUP_PARAMS, "field", new long[]{1, 3, 4}, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testTargetedMetricFilter() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new TargetedMetricFilter(TEST_REGROUP_PARAMS, stats, 1, 5, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testMetricRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(MetricRegroup.createMetricRegroup(TEST_REGROUP_PARAMS, stats, 1, 5, 3, true, SESSION_ID));
    }

    @Override
    @Test
    public void testMultiRegroup() throws IOException {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 0, "strTerm", false)})
        };
        assertEqualDeserialize(MultiRegroup.createMultiRegroupCommand(TEST_REGROUP_PARAMS, rawRules, true, SESSION_ID));
    }

    public void testMultiRegroupMessagesSender() throws IOException {
        // This command isn't serialized on the server side
    }

    public void testMultiRegroupMessagesIterator() throws IOException {
        // This command isn't serialized on the server side
    }

    @Override
    @Test
    public void testUntargetedMetricFilter() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new UntargetedMetricFilter(TEST_REGROUP_PARAMS, stats, 0, 5, true, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomMetricMultiRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new RandomMetricMultiRegroup(TEST_REGROUP_PARAMS, stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomMetricRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new RandomMetricRegroup(TEST_REGROUP_PARAMS, stats, RANDOM_SALT, 0.4, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomMultiRegroup() throws IOException {
        assertEqualDeserialize(new RandomMultiRegroup(TEST_REGROUP_PARAMS, "fieldName", false, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Override
    @Test
    public void testRandomRegroup() throws IOException {
        assertEqualDeserialize(new RandomRegroup(TEST_REGROUP_PARAMS, "fieldName", true, RANDOM_SALT, 0.03, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testRegexRegroup() throws IOException {
        assertEqualDeserialize(new RegexRegroup(TEST_REGROUP_PARAMS, "fieldName", ".*.*", 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testQueryRegroup() throws IOException {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")), 1, 2);
        assertEqualDeserialize(new QueryRegroup(TEST_REGROUP_PARAMS, rule, SESSION_ID));
    }

    @Override
    @Test
    public void testUnconditionalRegroup() throws IOException {
        assertEqualDeserialize(new UnconditionalRegroup(TEST_REGROUP_PARAMS, new int[]{1, 2, 3}, new int[]{12, 43, 12}, true, SESSION_ID));
    }

    @Override
    @Test
    public void testStringOrRegroup() throws IOException {
        final List<String> terms = Lists.newArrayList("1");
        assertEqualDeserialize(new StringOrRegroup(TEST_REGROUP_PARAMS, "fieldName", terms, 1, 2, 3, SESSION_ID));
    }

    @Override
    @Test
    public void testConsolidateGroups() throws Exception {
        final List<String> inputGroups = Lists.newArrayList("groups1", "groups2");
        assertEqualDeserialize(new ConsolidateGroups(inputGroups, Operator.AND, "outputGroups", SESSION_ID));
    }

    @Override
    @Test
    public void testResetGroups() throws Exception {
        assertEqualDeserialize(new ResetGroups("foo", SESSION_ID));
    }

    @Override
    @Test
    public void testDeleteGroups() throws Exception {
        assertEqualDeserialize(new DeleteGroups(Collections.singletonList("someGroups"), SESSION_ID));
    }

}
