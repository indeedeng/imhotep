package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.DatasetInfoMessage;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.QueryRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.imhotep.protobuf.ShardInfoMessage;
import com.indeed.imhotep.service.InputStreamDocIterator;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author jsgroth
 *
 * an ImhotepSession for talking to a remote ImhotepDaemon over a Socket using protobufs
 */
public class ImhotepRemoteSession extends AbstractImhotepSession {
    private static final Logger log = Logger.getLogger(ImhotepRemoteSession.class);

    public static final int DEFAULT_MERGE_THREAD_LIMIT = ImhotepRequest.getDefaultInstance().getMergeThreadLimit();

    private static final int DEFAULT_SOCKET_TIMEOUT = (int)TimeUnit.MINUTES.toMillis(30);

    private static final int CURRENT_CLIENT_VERSION = 2; // id to be incremented as changes to the client are done

    private final String host;
    private final int port;
    private final String sessionId;
    private final int socketTimeout;

    private int numStats = 0;

    public ImhotepRemoteSession(String host, int port, String sessionId) {
        this(host, port, sessionId, DEFAULT_SOCKET_TIMEOUT);
    }
    
    public ImhotepRemoteSession(String host, int port, String sessionId, int socketTimeout) {
        this.host = host;
        this.port = port;
        this.sessionId = sessionId;
        this.socketTimeout = socketTimeout;
    }

    @Deprecated
    public static List<ShardInfo> getShardList(final String host, final int port) throws IOException {
        log.trace("sending get shard request to "+host+":"+port);
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_SHARD_LIST)
                .build();

        final ImhotepResponse response = sendRequest(request, host, port);

        final List<ShardInfoMessage> protoShardInfo = response.getShardInfoList();
        final List<ShardInfo> ret = new ArrayList<ShardInfo>(protoShardInfo.size());
        for (final ShardInfoMessage shardInfo : protoShardInfo) {
            ret.add(ShardInfo.fromProto(shardInfo));
        }
        return ret;
    }

    public static List<DatasetInfo> getShardInfoList(final String host, final int port) throws IOException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_SHARD_INFO_LIST)
                .build();

        final ImhotepResponse response = sendRequest(request, host, port);

        final List<DatasetInfoMessage> protoShardInfo = response.getDatasetInfoList();
        final List<DatasetInfo> ret = Lists.newArrayListWithCapacity(protoShardInfo.size());
        for (final DatasetInfoMessage datasetInfo : protoShardInfo) {
            ret.add(DatasetInfo.fromProto(datasetInfo));
        }
        return ret;
    }

    public static ImhotepStatusDump getStatusDump(final String host, final int port) throws IOException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_STATUS_DUMP)
                .build();

        final ImhotepResponse response = sendRequest(request, host, port);

        return ImhotepStatusDump.fromProto(response.getStatusDump());
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, DEFAULT_MERGE_THREAD_LIMIT, getUsername(), false, -1);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards,
                                                   final int mergeThreadLimit) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, mergeThreadLimit, getUsername(), false, -1);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards,
                                                   final int mergeThreadLimit, final String username,
                                                   final boolean optimizeGroupZeroLookups, final int socketTimeout) throws ImhotepOutOfMemoryException, IOException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());

        try {
            log.trace("sending open request to "+host+":"+port+" for shards "+shards);
            final ImhotepRequest openSessionRequest = getBuilderForType(ImhotepRequest.RequestType.OPEN_SESSION)
                    .setUsername(username)
                    .setDataset(dataset)
                    .setMergeThreadLimit(mergeThreadLimit)
                    .addAllShardRequest(shards)
                    .setOptimizeGroupZeroLookups(optimizeGroupZeroLookups)
                    .setClientVersion(CURRENT_CLIENT_VERSION)
                    .build();
            try {
                ImhotepProtobufShipping.sendProtobuf(openSessionRequest, os);
    
                log.trace("waiting for confirmation from "+host+":"+port);
                final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
                if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
                    throw new ImhotepOutOfMemoryException();
                } else if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                    throw buildExceptionFromResponse(response, host, port);
                }
                final String sessionId = response.getSessionId();
    
                log.trace("session created, id "+sessionId);
                return new ImhotepRemoteSession(host, port, sessionId, socketTimeout);
            } catch (SocketTimeoutException e) {
                throw buildExceptionAfterSocketTimeout(e, host, port);
            }
        } finally {
            closeSocket(socket, is, os);
        }
    }

    public static String getUsername() {
        return System.getProperty("user.name");
    }

    @Override
    public long getTotalDocFreq(String[] intFields, String[] stringFields) {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_TOTAL_DOC_FREQ)
                .setSessionId(sessionId)
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            return response.getTotalDocFreq();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long[] getGroupStats(int stat) {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_GROUP_STATS)
                .setSessionId(sessionId)
                .setStat(stat)
                .build();
        final ImhotepResponse response;
        try {
            response = sendRequest(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final List<Long> groupStats = response.getGroupStatList();
        final long[] ret = new long[groupStats.size()];
        for (int i = 0; i < ret.length; ++i) {
            ret[i] = groupStats.get(i);
        }
        return ret;
    }

    @Override
    public FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields) {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_FTGS_ITERATOR)
                .setSessionId(sessionId)
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .build();

        try {
            final Socket socket = newSocket(host, port, socketTimeout);
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
            try {
                sendRequest(request, is, os, host, port);
            } catch (IOException e) {
                closeSocket(socket, is, os);
                throw e;
            }
            return new ClosingInputStreamFTGSIterator(socket, new ActiveBufferedInputStream(is, 8*1024*1024), os, numStats);
        } catch (IOException e) {
            throw new RuntimeException(e); // TODO
        }
    }

    public DocIterator getDocIterator(final String[] intFields, final String[] stringFields) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_DOC_ITERATOR)
                .setSessionId(sessionId)
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .build();
        try {
            final Socket socket = newSocket(host, port, socketTimeout);
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
            try {
                sendRequest(request, is, os, host, port);
            } catch (IOException e) {
                closeSocket(socket, is, os);
                throw e;
            }
            return new InputStreamDocIterator(is, intFields.length, stringFields.length);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields, final int numSplits) {
        final FTGSIterator ftgsIterator = getFTGSIterator(intFields, stringFields);
        try {
            final FTGSSplitter splitter = new FTGSSplitter(ftgsIterator, numSplits, numStats);
            return splitter.getFtgsIterators();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public int regroup(GroupMultiRemapRule[] rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        try {
            final ImhotepResponse response = sendMultisplitRegroupRequest(rawRules, sessionId, errorOnCollisions);
            return response.getNumGroups();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int regroup(int numRawRules, Iterator<GroupMultiRemapRule> rawRules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        try {
            final ImhotepResponse response = sendMultisplitRegroupRequest(numRawRules, rawRules, sessionId, errorOnCollisions);
            return response.getNumGroups();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int regroup(GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        final List<GroupRemapMessage> protoRules = ImhotepClientMarshaller.marshal(rawRules);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.REGROUP)
                .setSessionId(sessionId)
                .addAllRemapRules(protoRules)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            return response.getNumGroups();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int regroup(QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        final QueryRemapMessage protoRule = ImhotepClientMarshaller.marshal(rule);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.QUERY_REGROUP)
                .setSessionId(sessionId)
                .setQueryRemapRule(protoRule)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            return response.getNumGroups();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void intOrRegroup(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.INT_OR_REGROUP)
                .setSessionId(sessionId)
                .setField(field)
                .addAllIntTerm(Longs.asList(terms))
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stringOrRegroup(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.STRING_OR_REGROUP)
                .setSessionId(sessionId)
                .setField(field)
                .addAllStringTerm(Arrays.asList(terms))
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void randomRegroup(String field, boolean isIntField, String salt, double p, int targetGroup, int negativeGroup,
                              int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RANDOM_REGROUP)
                .setSessionId(sessionId)
                .setField(field)
                .setIsIntField(isIntField)
                .setSalt(salt)
                .setP(p)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void randomMultiRegroup(String field, boolean isIntField, String salt, int targetGroup, double[] percentages,
                                   int[] resultGroups) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RANDOM_MULTI_REGROUP)
                .setSessionId(sessionId)
                .setField(field)
                .setIsIntField(isIntField)
                .setSalt(salt)
                .setTargetGroup(targetGroup)
                .addAllPercentages(Doubles.asList(percentages))
                .addAllResultGroups(Ints.asList(resultGroups))
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int metricRegroup(int stat, long min, long max, long intervalSize) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_REGROUP)
                .setSessionId(sessionId)
                .setXStat(stat)
                .setXMin(min)
                .setXMax(max)
                .setXIntervalSize(intervalSize)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            return response.getNumGroups();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int metricRegroup2D(int xStat, long xMin, long xMax, long xIntervalSize, int yStat, long yMin, long yMax, long yIntervalSize) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_REGROUP_2D)
                .setSessionId(sessionId)
                .setXStat(xStat)
                .setXMin(xMin)
                .setXMax(xMax)
                .setXIntervalSize(xIntervalSize)
                .setYStat(yStat)
                .setYMin(yMin)
                .setYMax(yMax)
                .setYIntervalSize(yIntervalSize)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            return response.getNumGroups();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int metricFilter(int stat, long min, long max, boolean negate) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_FILTER)
                .setSessionId(sessionId)
                .setXStat(stat)
                .setXMin(min)
                .setXMax(max)
                .setNegate(negate)
                .build();
        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            return response.getNumGroups();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<TermCount> approximateTopTerms(String field, boolean isIntField, int k) {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.APPROXIMATE_TOP_TERMS)
                .setSessionId(sessionId)
                .setField(field)
                .setIsIntField(isIntField)
                .setK(k)
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            return ImhotepClientMarshaller.marshal(response.getTopTermsList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int pushStat(String statName) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.PUSH_STAT)
                .setSessionId(sessionId)
                .setMetric(statName)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            numStats = response.getNumStats();
            return numStats;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        for (String statName : statNames) {
            this.pushStat(statName);
        }

        return numStats;
    }

    @Override
    public int popStat() {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.POP_STAT)
                .setSessionId(sessionId)
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            numStats = response.getNumStats();
            return numStats;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getNumStats() {
        // TODO: really should ask the remote session just to be sure.
        return numStats;
    }

    @Override
    public void createDynamicMetric(String name) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.CREATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateDynamicMetric(String name, int[] deltas) {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.UPDATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();

        try {
            sendRequest(request, host, port);
        } catch (SocketTimeoutException e) {
            throw new RuntimeException(buildExceptionAfterSocketTimeout(e, host, port));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void conditionalUpdateDynamicMetric(String name, RegroupCondition[] conditions, int[] deltas) {
        List<RegroupConditionMessage> conditionMessages = ImhotepClientMarshaller.marshal(conditions);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.CONDITIONAL_UPDATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .addAllConditions(conditionMessages)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();
        try {
            sendRequest(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rebuildAndFilterIndexes(List<String> intFields, List<String> stringFields) throws ImhotepOutOfMemoryException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.OPTIMIZE_SESSION)
                .setSessionId(sessionId)
                .addAllIntFields(intFields)
                .addAllStringFields(stringFields)
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.CLOSE_SESSION)
                .setSessionId(sessionId)
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
        } catch (IOException e) {
            log.error("error closing session", e);
        }
    }

    @Override
    public void resetGroups() {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RESET_GROUPS)
                .setSessionId(sessionId)
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getHost() {
        return host;
    }

    private static ImhotepRequest.Builder getBuilderForType(ImhotepRequest.RequestType requestType) {
        return ImhotepRequest.newBuilder().setRequestType(requestType);
    }

    private static ImhotepResponse sendRequest(ImhotepRequest request, String host, int port) throws IOException {
        return sendRequest(request, host, port, -1);
    }
    
    private static ImhotepResponse sendRequest(ImhotepRequest request, String host, int port, int socketTimeout) throws IOException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        try {
            return sendRequest(request, is, os, host, port);
        } catch (IOException e) {
            log.error("error sending " + request.getRequestType() + " request to " + host + ":" + port, e);
            throw e;
        } finally {
            closeSocket(socket, is, os);
        }
    }

    // Special cased in order to save memory and only have one marshalled rule exist at a time.
    private ImhotepResponse sendMultisplitRegroupRequest(GroupMultiRemapRule[] rules, String sessionId, boolean errorOnCollisions) throws IOException, ImhotepOutOfMemoryException {
        return sendMultisplitRegroupRequest(rules.length, Arrays.asList(rules).iterator(), sessionId, errorOnCollisions);
    }

    private ImhotepResponse sendMultisplitRegroupRequest(int numRules, Iterator<GroupMultiRemapRule> rules, String sessionId, boolean errorOnCollisions) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepRequest initialRequest = getBuilderForType(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP)
                .setLength(numRules)
                .setSessionId(sessionId)
                .setErrorOnCollisions(errorOnCollisions)
                .build();

        final Socket socket = newSocket(host, port, socketTimeout);
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        try {
            ImhotepProtobufShipping.sendProtobuf(initialRequest, os);
            while (rules.hasNext()) {
                final GroupMultiRemapRule rule = rules.next();
                final GroupMultiRemapMessage ruleMessage = ImhotepClientMarshaller.marshal(rule);
                ImhotepProtobufShipping.sendProtobuf(ruleMessage, os);
            }
            return readResponseWithMemoryException(is, host, port);
        } catch (IOException e) {
            log.error("error sending exploded multisplit regroup request to " + host + ":" + port, e);
            throw e;
        } finally {
            closeSocket(socket, is, os);
        }
    }

    private static ImhotepResponse sendRequestWithMemoryException(ImhotepRequest request, String host, int port, int socketTimeout) throws IOException, ImhotepOutOfMemoryException {
        ImhotepResponse response = sendRequest(request, host, port);
        if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
            throw new ImhotepOutOfMemoryException();
        } else {
            return response;
        }
    }

    private static ImhotepResponse sendRequest(ImhotepRequest request, InputStream is, OutputStream os, String host, int port) throws IOException {
        try {
            ImhotepProtobufShipping.sendProtobuf(request, os);
            final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
            if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildExceptionFromResponse(response, host, port);
            }
            return response;
        } catch (SocketTimeoutException e) {
            throw buildExceptionAfterSocketTimeout(e, host, port);
        }
    }

    private static ImhotepResponse readResponseWithMemoryException(InputStream is, String host, int port) throws IOException, ImhotepOutOfMemoryException {
        try {
            final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
            if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildExceptionFromResponse(response, host, port);
            } else if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
                throw new ImhotepOutOfMemoryException();
            } else {
                return response;
            }
        } catch (SocketTimeoutException e) {
            throw buildExceptionAfterSocketTimeout(e, host, port);
        }
    }

    private static IOException buildExceptionFromResponse(ImhotepResponse response, String host, int port) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" returned error: ").append(response.getExceptionType())
                .append(": ").append(response.getExceptionMessage());
        return new IOException(msg.toString());
    }
    
    private static IOException buildExceptionAfterSocketTimeout(SocketTimeoutException e, String host, int port) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" socket timed out: ").append(e.getMessage());
        
        return new IOException(msg.toString());
    }

    private static void closeSocket(Socket socket, InputStream is, OutputStream os) {
        try {
            if (os != null) {
                os.close();
            }
        } catch (IOException e) {
            log.error(e);
        }
        try {
            if (is != null) {
                is.close();
            }
        } catch (IOException e) {
            log.error(e);
        }
        try {
            socket.close();
        } catch (IOException e) {
            log.error(e);
        }
    }

    private static Socket newSocket(String host, int port) throws IOException {
        return newSocket(host, port, DEFAULT_SOCKET_TIMEOUT);
    }

    private static Socket newSocket(String host, int port, int timeout) throws IOException {
        final Socket socket = new Socket(host, port);
        socket.setReceiveBufferSize(65536);
        socket.setSoTimeout(timeout >= 0 ? timeout : DEFAULT_SOCKET_TIMEOUT);
        socket.setTcpNoDelay(true);
        return socket;
    }
}
