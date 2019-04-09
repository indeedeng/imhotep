package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepCommand;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * Validating deserialization of ImhotepCommand gives expected command and parameters.
 */
public class TestImhotepCommandDeserialization implements CommandsTest {

    public static final String SESSION_ID = "RandomSessionIdString";
    public static final String RANDOM_SALT = "RandomSaltString";

    private void assertEqualDeserialize(final ImhotepCommand command) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        command.writeToOutputStream(outputStream);
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ImhotepCommand deserializedCommand =  ImhotepCommand.readFromInputStream(inputStream);
        Assert.assertEquals(0, inputStream.available());
        Assert.assertEquals(command, deserializedCommand);
    }

    @Test
    public void testGetGroupStats() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new GetGroupStats(stats, SESSION_ID));
    }

    @Test
    public void testIntOrRegroup() throws IOException {
        assertEqualDeserialize(new IntOrRegroup( "field", new long[]{1, 3, 4}, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testMetricFilter() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new MetricFilter(stats, 1, 5, 1, 2, 3, SESSION_ID));
    }


    @Test
    public void testMetricRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(MetricRegroup.createMetricRegroup(stats, 1, 5, 3, true, SESSION_ID));
    }

    @Test
    public void testMultiRegroup() throws IOException {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 0, "strTerm", false)})
        };
        assertEqualDeserialize(MultiRegroup.createMultiRegroupCommand(rawRules, true , SESSION_ID));
    }

    public void testMultiRegroupMessagesSender() throws IOException {
        // This command isn't serialized on the server side
    }

    public void testMultiRegroupMessagesIterator() throws IOException {
         // This command isn't serialized on the server side
    }

    @Test
    public void testNegateMetricFilter() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new NegateMetricFilter(stats, 0, 5, true, SESSION_ID));
    }

    @Test
    public void testRandomMetricMultiRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new RandomMetricMultiRegroup(stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Test
    public void testRandomMetricRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        assertEqualDeserialize(new RandomMetricRegroup(stats, RANDOM_SALT, 0.4, 1, 2, 3, SESSION_ID));
    }


    @Test
    public void testRandomMultiRegroup() throws IOException {
        assertEqualDeserialize(new RandomMultiRegroup("fieldName", false, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Test
    public void testRandomRegroup() throws IOException {
        assertEqualDeserialize(new RandomRegroup("fieldName", true, RANDOM_SALT, 0.03, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testRegexRegroup() throws IOException {
        assertEqualDeserialize(new RegexRegroup("fieldName", ".*.*", 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testRegroup() throws IOException {
        final GroupRemapRule[] rawRules = new GroupRemapRule[] {
                new GroupRemapRule(1, new RegroupCondition("fieldName", false, 0, "strTerm", false), 1000000, 1000000)
        };
        assertEqualDeserialize(Regroup.createRegroup(rawRules, SESSION_ID));
    }

    @Test
    public void testQueryRegroup() throws IOException {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")),1, 2);
        assertEqualDeserialize(new QueryRegroup(rule, SESSION_ID));
    }

    @Test
    public void testUnconditionalRegroup() throws IOException {
        assertEqualDeserialize(new UnconditionalRegroup(new int[]{1, 2, 3}, new int[]{12, 43, 12}, true, SESSION_ID));
    }

    @Test
    public void testStringOrRegroup() throws IOException {
        final List<String> terms = Lists.newArrayList("1");
        assertEqualDeserialize(new StringOrRegroup("fieldName", terms, 1, 2, 3, SESSION_ID));
    }

}
