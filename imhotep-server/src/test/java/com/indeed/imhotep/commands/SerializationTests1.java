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

public class SerializationTests1 implements CommandsTest {

    public static final String SESSION_ID = "RandomSessionIdString";
    public static final String RANDOM_SALT = "RandomSaltString";

    private ImhotepCommand getDeserialized(final ImhotepCommand command) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        command.writeToOutputStream(outputStream);
        outputStream.flush();
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        final ImhotepCommand deserializedCommand =  ImhotepCommand.readFromInputStream(inputStream);
        Assert.assertEquals(0, inputStream.available());
        return deserializedCommand;
    }

    @Test
    public void testGetGroupStats() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        final GetGroupStats getGroupStats = new GetGroupStats(stats, SESSION_ID);
        Assert.assertEquals(getGroupStats, getDeserialized(getGroupStats));
    }

    @Test
    public void testIntOrRegroup() throws IOException {
        final IntOrRegroup intOrRegroup = new IntOrRegroup( "field", new long[]{1,3,4}, 1, 2, 3, SESSION_ID);
        Assert.assertEquals(intOrRegroup, getDeserialized(intOrRegroup));
    }

    @Test
    public void testMetricFilter() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        final MetricFilter metricFilter = new MetricFilter(stats, 1, 5, 1, 2, 3, SESSION_ID);
        Assert.assertEquals(metricFilter, getDeserialized(metricFilter));
    }


    @Test
    public void testMetricRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        final MetricRegroup metricRegroup = MetricRegroup.createMetricRegroup(stats, 1, 5, 3, true, SESSION_ID);
        Assert.assertEquals(metricRegroup, getDeserialized(metricRegroup));
    }

    @Test
    public void testMultiRegroup() throws IOException {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 0, "strTerm", false)})
        };
        final MultiRegroup multiRegroup = MultiRegroup.createMultiRegroupCommand(rawRules, true , SESSION_ID);
        final MultiRegroup deserialized = ((MultiRegroup) getDeserialized(multiRegroup));
        Assert.assertEquals(multiRegroup, deserialized);
    }

    @Test
    public void testMultiRegroupMessagesSender() throws IOException {
        // This command isn't serialized on the server side
    }

    @Test
    public void testMultiRegroupMessagesIterator() throws IOException {
         // This command isn't serialized on the server side
    }

    @Test
    public void testNegateMetricFilter() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        final NegateMetricFilter negateMetricFilter = new NegateMetricFilter(stats, 0, 5, true, SESSION_ID);
        Assert.assertEquals(negateMetricFilter, getDeserialized(negateMetricFilter));
    }

    @Test
    public void testRandomMetricMultiRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        final RandomMetricMultiRegroup randomMetricMultiRegroup = new RandomMetricMultiRegroup(stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID);
        Assert.assertEquals(randomMetricMultiRegroup, getDeserialized(randomMetricMultiRegroup));
    }

    @Test
    public void testRandomMetricRegroup() throws IOException {
        final List<String> stats = Lists.newArrayList("1");
        final RandomMetricRegroup randomMetricRegroup = new RandomMetricRegroup(stats, RANDOM_SALT, 0.4, 1, 2, 3, SESSION_ID);
        Assert.assertEquals(randomMetricRegroup, getDeserialized(randomMetricRegroup));
    }


    @Test
    public void testRandomMultiRegroup() throws IOException {
        final RandomMultiRegroup randomMultiRegroup = new RandomMultiRegroup("fieldName", false, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID);
        Assert.assertEquals(randomMultiRegroup, getDeserialized(randomMultiRegroup));
    }

    @Test
    public void testRandomRegroup() throws IOException {
        final RandomRegroup randomRegroup = new RandomRegroup("fieldName", true, RANDOM_SALT, 0.03, 1, 2, 3, SESSION_ID);
        Assert.assertEquals(randomRegroup, getDeserialized(randomRegroup));
    }

    @Test
    public void testRegexRegroup() throws IOException {
        final RegexRegroup regexRegroup = new RegexRegroup("fieldName", ".*.*", 1, 2, 3, SESSION_ID);
        Assert.assertEquals(regexRegroup, getDeserialized(regexRegroup));
    }

    @Test
    public void testRegroup() throws IOException {
        final GroupRemapRule[] rawRules = new GroupRemapRule[] {
                new GroupRemapRule(1, new RegroupCondition("fieldName", false, 0, "strTerm", false), 1000000, 1000000)
        };
        final Regroup regroup = Regroup.createRegroup(rawRules, SESSION_ID);
        Assert.assertEquals(regroup, getDeserialized(regroup));
    }

    @Test
    public void testQueryRemapRuleRegroup() throws IOException {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")),1, 2);
        final QueryRemapRuleRegroup queryRemapRuleRegroup = new QueryRemapRuleRegroup(rule, SESSION_ID);
        Assert.assertEquals(queryRemapRuleRegroup, getDeserialized(queryRemapRuleRegroup));
    }

    @Test
    public void testUnconditionalRegroup() throws IOException {
        final UnconditionalRegroup unconditionalRegroup = new UnconditionalRegroup(new int[]{1,2,3}, new int[]{12,43,12}, true, SESSION_ID);
        Assert.assertEquals(unconditionalRegroup, getDeserialized(unconditionalRegroup));
    }

    @Test
    public void testStringOrRegroup() throws IOException {
        final List<String> terms = Lists.newArrayList("1");
        final StringOrRegroup stringOrRegroup = new StringOrRegroup("fieldName", terms, 1, 2, 3, SESSION_ID);
        Assert.assertEquals(stringOrRegroup, getDeserialized(stringOrRegroup));
    }

}
