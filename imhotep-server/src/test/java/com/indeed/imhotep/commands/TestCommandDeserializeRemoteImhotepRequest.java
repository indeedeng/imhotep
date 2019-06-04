package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RegroupParams;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.Operator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Compares request sent by ImhotepRemoteSession with the Command deserialized by ImhotepCommand
 */
public class TestCommandDeserializeRemoteImhotepRequest implements CommandsTest {

    private static final String SESSION_ID = "RandomSessionIdString";
    private static final String RANDOM_SALT = "RandomSaltString";
    private static final String TEST_INPUT_GROUPS_NAME = "myOtherTestGroups";
    private static final RegroupParams TEST_REGROUP_PARAMS = new RegroupParams("myInputGroups", "myOutputGroups");

    private ImhotepRemoteSession imhotepRemoteSession;
    private HostPortCommandHolder holder;
    private AtomicLong tempFileSizeHolder = new AtomicLong(100);

    private class HostPortCommandHolder implements Closeable {
        String host;
        int port;
        private final Thread thread;
        private final ServerSocket serverSocket;
        final CompletableFuture<ImhotepCommand> futureCommand;

        public HostPortCommandHolder(final String host, final int port, final Thread thread, final ServerSocket serverSocket, final CompletableFuture<ImhotepCommand> futureCommand) {
            this.host = host;
            this.port = port;
            this.thread = thread;
            this.serverSocket = serverSocket;
            this.futureCommand = futureCommand;
        }

        @Override
        public void close() throws IOException {
            thread.stop();
            serverSocket.close();
        }
    }

    /**
     * Each test makes call from imhotepRemoteSession, and deserialize the request to an ImhotepCommand and asserts that we receive expected command
     * ImhotepRemoteSession expects a response after sending the request.
     * This method creates a new thread to send a dummy response and deserialize the request received from ImhotepRemoteSession
     * We need to return a Future Object for the deserialized ImhotepCommand because we need to close socket for the
     * imhotepRemoteSession call to indicate end of stream.
     */
    private HostPortCommandHolder sendDummyResponseReceiveCommand() throws IOException {
        final ServerSocket serverSocket = new ServerSocket(0);
        final CompletableFuture<ImhotepCommand> futureCommand = new CompletableFuture<>();
        final Thread requestReceiver = new Thread(new Runnable() {
            @Override
            public void run() {
                try (final Socket socket = serverSocket.accept()) {
                    ImhotepProtobufShipping.sendProtobuf(ImhotepResponse.newBuilder().setNumGroups(0).build(), socket.getOutputStream());
                    futureCommand.complete(ImhotepCommand.readFromInputStream(socket.getInputStream()));
                } catch (IOException e) {
                    e.printStackTrace();
                    futureCommand.completeExceptionally(e);
                }
            }
        });
        requestReceiver.start();
        return new HostPortCommandHolder("localhost", serverSocket.getLocalPort(), requestReceiver, serverSocket, futureCommand);
    }

    @Before
    public void initialize() throws IOException {
        holder = sendDummyResponseReceiveCommand();
        imhotepRemoteSession = new ImhotepRemoteSession(holder.host, holder.port, SESSION_ID, tempFileSizeHolder);
    }

    @Override
    @Test
    public void testGetGroupStats() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.getGroupStatsIterator(TEST_INPUT_GROUPS_NAME, stats);
        Assert.assertEquals(new GetGroupStats(TEST_INPUT_GROUPS_NAME, stats, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testIntOrRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final String field = "field";
        final long[] terms = new long[]{1, 3};
        imhotepRemoteSession.intOrRegroup(TEST_REGROUP_PARAMS, field, terms, 1, 2, 3);
        Assert.assertEquals(new IntOrRegroup(TEST_REGROUP_PARAMS, field, terms, 1, 2, 3, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testTargetedMetricFilter() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.metricFilter(TEST_REGROUP_PARAMS, stats, 0, 100, 1, 2, 3);
        Assert.assertEquals(new TargetedMetricFilter(TEST_REGROUP_PARAMS, stats, 0, 100, 1, 2, 3, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testMetricRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.metricRegroup(TEST_REGROUP_PARAMS, stats, 0, 100, 10, false);
        Assert.assertEquals(MetricRegroup.createMetricRegroup(TEST_REGROUP_PARAMS, stats, 0, 100, 10, false, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testMultiRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[]{
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 0, "strTerm", false)})
        };
        imhotepRemoteSession.regroup(TEST_REGROUP_PARAMS, rawRules, true);
        Assert.assertEquals(MultiRegroup.createMultiRegroupCommand(TEST_REGROUP_PARAMS, rawRules, true, SESSION_ID), holder.futureCommand.get());
    }

    public void testMultiRegroupMessagesSender() throws IOException {
        // This command isn't serialized on the server side
    }

    public void testMultiRegroupMessagesIterator() throws IOException {
        // This command isn't serialized on the server side
    }

    @Override
    @Test
    public void testUntargetedMetricFilter() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.metricFilter(TEST_REGROUP_PARAMS, stats, 0, 5, true);
        Assert.assertEquals(new UntargetedMetricFilter(TEST_REGROUP_PARAMS, stats, 0, 5, true, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testRandomMetricMultiRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.randomMetricMultiRegroup(TEST_REGROUP_PARAMS, stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6});
        Assert.assertEquals(new RandomMetricMultiRegroup(TEST_REGROUP_PARAMS, stats, "RandomSaltString", 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testRandomMetricRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.randomMetricRegroup(TEST_REGROUP_PARAMS, stats, RANDOM_SALT, 0.3, 1, 2, 3);
        Assert.assertEquals(new RandomMetricRegroup(TEST_REGROUP_PARAMS, stats, RANDOM_SALT, 0.3, 1, 2, 3, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testRandomMultiRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        imhotepRemoteSession.randomMultiRegroup(TEST_REGROUP_PARAMS, "field", true, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6});
        Assert.assertEquals(new RandomMultiRegroup(TEST_REGROUP_PARAMS, "field", true, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testRandomRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        imhotepRemoteSession.randomRegroup(TEST_REGROUP_PARAMS, "field", true, RANDOM_SALT, 0.3, 1, 2, 3);
        Assert.assertEquals(new RandomRegroup(TEST_REGROUP_PARAMS, "field", true, RANDOM_SALT, 0.3, 1, 2, 3, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testRegexRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        imhotepRemoteSession.regexRegroup(TEST_REGROUP_PARAMS, "field", ".*.*", 1, 2, 3);
        Assert.assertEquals(new RegexRegroup(TEST_REGROUP_PARAMS, "field", ".*.*", 1, 2, 3, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testQueryRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")), 1, 2);
        imhotepRemoteSession.regroup(TEST_REGROUP_PARAMS, rule);
        Assert.assertEquals(new QueryRegroup(TEST_REGROUP_PARAMS, rule, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testUnconditionalRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final UnconditionalRegroup unconditionalRegroup = new UnconditionalRegroup(TEST_REGROUP_PARAMS, new int[]{1, 2, 3}, new int[]{12, 43, 12}, true, SESSION_ID);
        imhotepRemoteSession.regroup(TEST_REGROUP_PARAMS, new int[]{1, 2, 3}, new int[]{12, 43, 12}, true);
        Assert.assertEquals(unconditionalRegroup, holder.futureCommand.get());
    }

    @Override
    @Test
    public void testStringOrRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> terms = Lists.newArrayList("1");
        imhotepRemoteSession.stringOrRegroup(TEST_REGROUP_PARAMS, "field", terms.toArray(new String[0]), 1, 2, 3);
        Assert.assertEquals(new StringOrRegroup(TEST_REGROUP_PARAMS, "field", terms, 1, 2, 3, SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testConsolidateGroups() throws Exception {
        final List<String> inputGroups = Lists.newArrayList("groups1", "groups2");
        imhotepRemoteSession.consolidateGroups(inputGroups, Operator.AND, "outputGroups");
        Assert.assertEquals(new ConsolidateGroups(inputGroups, Operator.AND, "outputGroups", SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testResetGroups() throws Exception {
        imhotepRemoteSession.resetGroups("foo");
        Assert.assertEquals(new ResetGroups("foo", SESSION_ID), holder.futureCommand.get());
    }

    @Override
    @Test
    public void testDeleteGroups() throws Exception {
        final List<String> groupsToDelete = Collections.singletonList("someGroups");
        imhotepRemoteSession.deleteGroups(groupsToDelete);
        Assert.assertEquals(new DeleteGroups(groupsToDelete, SESSION_ID), holder.futureCommand.get());
    }
}
