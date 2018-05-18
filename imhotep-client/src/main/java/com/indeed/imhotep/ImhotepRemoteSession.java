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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.LimitedBufferedOutputStream;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.io.TempFileSizeLimitExceededException;
import com.indeed.imhotep.io.WriteLimitExceededException;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.DatasetInfoMessage;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.IntFieldAndTerms;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.imhotep.protobuf.QueryRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.imhotep.protobuf.ShardInfoMessage;
import com.indeed.imhotep.protobuf.StringFieldAndTerms;
import com.indeed.imhotep.service.InputStreamDocIterator;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import it.unimi.dsi.fastutil.longs.LongIterators;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.indeed.imhotep.protobuf.ImhotepRequest.RequestType.GET_FTGS_SPLIT;
import static com.indeed.imhotep.protobuf.ImhotepRequest.RequestType.GET_FTGS_SPLIT_NATIVE;

/**
 * @author jsgroth
 *
 * an ImhotepSession for talking to a remote ImhotepDaemon over a Socket using protobufs
 */
public class ImhotepRemoteSession
    extends AbstractImhotepSession
    implements HasSessionId {
    private static final Logger log = Logger.getLogger(ImhotepRemoteSession.class);

    public static final int DEFAULT_MERGE_THREAD_LIMIT =
        ImhotepRequest.getDefaultInstance().getMergeThreadLimit();

    private static final int DEFAULT_SOCKET_TIMEOUT = (int)TimeUnit.MINUTES.toMillis(30);

    private static final int CURRENT_CLIENT_VERSION = 2; // id to be incremented as changes to the client are done

    private final String host;
    private final int port;
    private final String sessionId;
    private final int socketTimeout;
    private final AtomicLong tempFileSizeBytesLeft;
    private final boolean useNativeFtgs;
    private boolean closed = false;

    private int numStats = 0;

    private final long numDocs;

    // cached for use by SubmitRequestEvent
    private final String sourceAddr;
    private final String targetAddr;

    private final class SubmitRequestEvent extends Instrumentation.Event {

        public SubmitRequestEvent(final ImhotepRequest request,
                                  final long           beginTimeMillis,
                                  final long           elapsedTimeMillis) {
            this(request.getRequestType(), beginTimeMillis, elapsedTimeMillis);
        }

        public SubmitRequestEvent(final ImhotepRequest.RequestType requestType,
                                  final long                       beginTimeMillis,
                                  final long                       elapsedTimeMillis) {
            super(SubmitRequestEvent.class.getSimpleName());
            getProperties().put(Keys.SESSION_ID,          ImhotepRemoteSession.this.sessionId);
            getProperties().put(Keys.REQUEST_TYPE,        requestType.toString());
            getProperties().put(Keys.BEGIN_TIME_MILLIS,   beginTimeMillis);
            getProperties().put(Keys.ELAPSED_TIME_MILLIS, elapsedTimeMillis);
            getProperties().put(Keys.SOURCE_ADDR,         ImhotepRemoteSession.this.sourceAddr);
            getProperties().put(Keys.TARGET_ADDR,         ImhotepRemoteSession.this.targetAddr);
        }
    }

    public ImhotepRemoteSession(final String host, final int port, final String sessionId,
                                final AtomicLong tempFileSizeBytesLeft, final boolean useNativeFtgs) {
        this(host, port, sessionId, tempFileSizeBytesLeft, DEFAULT_SOCKET_TIMEOUT, useNativeFtgs);
    }

    public ImhotepRemoteSession(final String host, final int port, final String sessionId,
                                @Nullable final AtomicLong tempFileSizeBytesLeft,
                                final int socketTimeout, final boolean useNativeFtgs) {
        this(host, port, sessionId, tempFileSizeBytesLeft, socketTimeout, useNativeFtgs, 0);
    }

    public ImhotepRemoteSession(final String host, final int port, final String sessionId,
                                @Nullable final AtomicLong tempFileSizeBytesLeft,
                                final int socketTimeout, final boolean useNativeFtgs, final long numDocs) {
        this.host = host;
        this.port = port;
        this.sessionId = sessionId;
        this.socketTimeout = socketTimeout;
        this.tempFileSizeBytesLeft = tempFileSizeBytesLeft;
        this.useNativeFtgs = useNativeFtgs;
        this.numDocs = numDocs;

        String tmpAddr;
        try {
            tmpAddr = InetAddress.getLocalHost().toString();
        }
        catch (final Exception ex) {
            tmpAddr = "";
            log.warn("cannot initialize sourceAddr", ex);
        }
        this.sourceAddr = tmpAddr;

        try {
            tmpAddr = InetAddress.getByName(host).toString();
        }
        catch (final Exception ex) {
            tmpAddr = host;
            log.warn("cannot initialize targetAddr", ex);
        }
        this.targetAddr = tmpAddr;
    }

    public String getSessionId() { return sessionId; }

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

    public static List<DatasetInfo> getDatasetMetadata(final String host, final int port) throws IOException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_DATASET_METADATA)
                .build();

        final ImhotepResponse response = sendRequest(request, host, port);

        final List<DatasetInfoMessage> protoShardInfo = response.getDatasetInfoList();
        final List<DatasetInfo> ret = Lists.newArrayListWithCapacity(protoShardInfo.size());
        for (final DatasetInfoMessage datasetInfo : protoShardInfo) {
            ret.add(DatasetInfo.fromProto(datasetInfo));
        }
        return ret;
    }

    public static List<ShardInfo> getShardlistForTime(final String host, final int port, final String dataset, final long startUnixtime, final long endUnixtime) throws IOException {
        if (endUnixtime <= startUnixtime) {
            throw new IllegalArgumentException("Start time must be before end time. Given start: " + startUnixtime + ", end: " + endUnixtime);
        }
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_SHARD_LIST_FOR_TIME)
                .setDataset(dataset)
                .setStartUnixtime(startUnixtime)
                .setEndUnixtime(endUnixtime)
                .build();

        final ImhotepResponse response = sendRequest(request, host, port);

        final List<ShardInfoMessage> protoShardInfo = response.getShardInfoList();
        final List<ShardInfo> ret = Lists.newArrayListWithCapacity(protoShardInfo.size());
        for (final ShardInfoMessage shardInfo : protoShardInfo) {
            ret.add(ShardInfo.fromProto(shardInfo));
        }
        return ret;
    }

    public static ImhotepStatusDump getStatusDump(final String host, final int port) throws IOException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_STATUS_DUMP)
                .build();

        final ImhotepResponse response = sendRequest(request, host, port);

        return ImhotepStatusDump.fromProto(response.getStatusDump());
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards, @Nullable final String sessionId, final long sessionTimeout) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, DEFAULT_MERGE_THREAD_LIMIT, getUsername(), false, -1, sessionId, -1, null, false, sessionTimeout);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards,
                                                   final int mergeThreadLimit, @Nullable final String sessionId, final long sessionTimeout) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, mergeThreadLimit, getUsername(), false, -1, sessionId, -1, null, false, sessionTimeout);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards,
                                                   final int mergeThreadLimit, final String username,
                                                   final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                                   @Nullable final String sessionId, final long tempFileSizeLimit,
                                                   @Nullable final AtomicLong tempFileSizeBytesLeft,
                                                   final boolean useNativeFtgs, final long sessionTimeout) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, mergeThreadLimit, username, optimizeGroupZeroLookups, socketTimeout, sessionId, tempFileSizeLimit, tempFileSizeBytesLeft, useNativeFtgs, sessionTimeout, 0);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards,
                                                   final int mergeThreadLimit, final String username,
                                                   final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                                   @Nullable final String sessionId, final long tempFileSizeLimit,
                                                   @Nullable final AtomicLong tempFileSizeBytesLeft,
                                                   final boolean useNativeFtgs, final long sessionTimeout, final long numDocs) throws ImhotepOutOfMemoryException, IOException {

        return openSession(host, port, dataset, shards, mergeThreadLimit, username, "", optimizeGroupZeroLookups, socketTimeout, sessionId, tempFileSizeLimit, tempFileSizeBytesLeft, useNativeFtgs, sessionTimeout, false, numDocs);
    }


    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<String> shards,
                                                   final int mergeThreadLimit, final String username, final String clientName,
                                                   final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                                   @Nullable String sessionId, final long tempFileSizeLimit,
                                                   @Nullable final AtomicLong tempFileSizeBytesLeft,
                                                   final boolean useNativeFtgs, final long sessionTimeout,
                                                   boolean allowSessionForwarding, final long numDocs) throws ImhotepOutOfMemoryException, IOException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());

        try {
            log.trace("sending open request to "+host+":"+port+" for shards "+shards);
            final ImhotepRequest openSessionRequest = getBuilderForType(ImhotepRequest.RequestType.OPEN_SESSION)
                    .setUsername(username)
                    .setClientName(clientName)
                    .setDataset(dataset)
                    .setMergeThreadLimit(mergeThreadLimit)
                    .addAllShardRequest(shards)
                    .setOptimizeGroupZeroLookups(optimizeGroupZeroLookups)
                    .setClientVersion(CURRENT_CLIENT_VERSION)
                    .setSessionId(sessionId == null ? "" : sessionId)
                    .setTempFileSizeLimit(tempFileSizeLimit)
                    .setUseNativeFtgs(useNativeFtgs)
                    .setSessionTimeout(sessionTimeout)
                    .setAllowSessionForwarding(allowSessionForwarding)
                    .build();
            try {
                ImhotepProtobufShipping.sendProtobuf(openSessionRequest, os);

                log.trace("waiting for confirmation from "+host+":"+port);
                final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
                if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
                    throw new ImhotepOutOfMemoryException();
                }
                if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                    throw buildExceptionFromResponse(response, host, port);
                }
                if (sessionId == null) {
                    sessionId = response.getSessionId();
                }

                final int actualPort;
                if (response.getNewPort() != 0) {
                    actualPort = response.getNewPort();
                } else {
                    actualPort = port;
                }

                log.trace("session created, id "+sessionId);
                return new ImhotepRemoteSession(host, actualPort, sessionId, tempFileSizeBytesLeft, socketTimeout, useNativeFtgs, numDocs);
            } catch (final SocketTimeoutException e) {
                throw buildExceptionAfterSocketTimeout(e, host, port);
            }
        } finally {
            closeSocket(socket);
        }
    }

    public static String getUsername() {
        return System.getProperty("user.name");
    }

    @Override
    public long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_TOTAL_DOC_FREQ)
                .setSessionId(sessionId)
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            final long result = response.getTotalDocFreq();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long[] getGroupStats(final int stat) {
        try (GroupStatsIterator reader = getGroupStatsIterator(stat)) {
            return LongIterators.unwrap(reader, reader.getNumGroups());
        } catch(final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final int stat) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.STREAMING_GET_GROUP_STATS)
                .setSessionId(sessionId)
                .setStat(stat)
                .build();
        return sendGroupStatsIteratorRequest(request, timer);
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields, final long termLimit, final int sortStat) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_FTGS_ITERATOR)
                .setSessionId(sessionId)
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .setTermLimit(termLimit)
                .setSortStat(sortStat)
                .build();

        final FTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.GET_SUBSET_FTGS_ITERATOR)
                .setSessionId(sessionId);
        addSubsetFieldsAndTermsToBuilder(intFields, stringFields, requestBuilder);
        final ImhotepRequest request = requestBuilder.build();
        final FTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    private void addSubsetFieldsAndTermsToBuilder(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final ImhotepRequest.Builder requestBuilder) {
        for (final Map.Entry<String, long[]> entry : intFields.entrySet()) {
            final IntFieldAndTerms.Builder builder = IntFieldAndTerms.newBuilder().setField(entry.getKey());
            for (final long term : entry.getValue()) {
                builder.addTerms(term);
            }
            requestBuilder.addIntFieldsToTerms(builder);
        }
        for (final Map.Entry<String, String[]> entry : stringFields.entrySet()) {
            requestBuilder.addStringFieldsToTerms(
                    StringFieldAndTerms.newBuilder()
                            .setField(entry.getKey())
                            .addAllTerms(Arrays.asList(entry.getValue()))
            );
        }
    }

    public DocIterator getDocIterator(final String[] intFields, final String[] stringFields) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
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
            } catch (final IOException e) {
                closeSocket(socket);
                throw e;
            }
            final DocIterator result = 
                new InputStreamDocIterator(is, intFields.length, stringFields.length);
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields, final long termLimit) {
        throw new UnsupportedOperationException();
    }

    public RawFTGSIterator getFTGSIteratorSplit(final String[] intFields, final String[] stringFields, final int splitIndex, final int numSplits, final long termLimit) {
        // TODO: disable timer to reduce logrepo logging volume of SubmitRequestEvent?
        final Timer timer = new Timer();
        final ImhotepRequest.RequestType requestType = useNativeFtgs ? GET_FTGS_SPLIT_NATIVE : GET_FTGS_SPLIT;
        final ImhotepRequest request = getBuilderForType(requestType)
                .setSessionId(sessionId)
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .setSplitIndex(splitIndex)
                .setNumSplits(numSplits)
                .setTermLimit(termLimit)
                .build();
        final RawFTGSIterator result = sendGetFTGSIteratorSplit(request);
        timer.complete(request);
        return result;
    }

    @Override
    public RawFTGSIterator[] getSubsetFTGSIteratorSplits(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RawFTGSIterator getSubsetFTGSIteratorSplit(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields,
            final int splitIndex,
            final int numSplits) {
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.GET_SUBSET_FTGS_SPLIT)
                .setSessionId(sessionId)
                .setSplitIndex(splitIndex)
                .setNumSplits(numSplits);
        addSubsetFieldsAndTermsToBuilder(intFields, stringFields, requestBuilder);
        final ImhotepRequest request = requestBuilder.build();
        final RawFTGSIterator result = sendGetFTGSIteratorSplit(request);
        timer.complete(request);
        return result;
    }

    private RawFTGSIterator sendGetFTGSIteratorSplit(final ImhotepRequest request) {
        try {
            final Socket socket = newSocket(host, port, socketTimeout);
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
            try {
                sendRequest(request, is, os, host, port);
            } catch (final IOException e) {
                closeSocket(socket);
                throw e;
            }
            return new ClosingInputStreamFTGSIterator(socket, is, os, numStats);
        } catch (final IOException e) {
            throw new RuntimeException(e); // TODO
        }
    }

    @Override
    public RawFTGSIterator mergeFTGSSplit(
            final String[] intFields,
            final String[] stringFields,
            final String sessionId,
            final InetSocketAddress[] nodes,
            final int splitIndex,
            final long termLimit,
            final int sortStat) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.MERGE_FTGS_SPLIT)
                .setSessionId(sessionId)
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .setSplitIndex(splitIndex)
                .setTermLimit(termLimit)
                .setSortStat(sortStat)
                .addAllNodes(Iterables.transform(Arrays.asList(nodes), new Function<InetSocketAddress, HostAndPort>() {
                    public HostAndPort apply(final InetSocketAddress input) {
                        return HostAndPort.newBuilder().setHost(input.getHostName()).setPort(input.getPort()).build();
                    }
                }))
                .build();

        final RawFTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    @Override
    public GroupStatsIterator mergeDistinctSplit(final String field,
                                                 final boolean isIntField,
                                                 final String sessionId,
                                                 final InetSocketAddress[] nodes,
                                                 final int splitIndex) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.MERGE_DISTINCT_SPLIT)
                .setSessionId(sessionId)
                .setField(field)
                .setIsIntField(isIntField)
                .setSplitIndex(splitIndex)
                .addAllNodes(Iterables.transform(Arrays.asList(nodes), new Function<InetSocketAddress, HostAndPort>() {
                    public HostAndPort apply(final InetSocketAddress input) {
                        return HostAndPort.newBuilder().setHost(input.getHostName()).setPort(input.getPort()).build();
                    }
                }))
                .build();
        return sendGroupStatsIteratorRequest(request, timer);
    }

    private GroupStatsIterator sendGroupStatsIteratorRequest(final ImhotepRequest request, final Timer timer) {
        try {
            final Socket socket = newSocket(host, port, socketTimeout);
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());

            final ImhotepResponse response = sendRequest(request, is, os, host, port);
            timer.complete(request);
            return ImhotepProtobufShipping.readGroupStatsIterator(is, response.getGroupStatSize());
        } catch(final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RawFTGSIterator mergeSubsetFTGSSplit(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields,
            final String sessionId,
            final InetSocketAddress[] nodes,
            final int splitIndex) {
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.MERGE_SUBSET_FTGS_SPLIT)
                .setSessionId(sessionId)
                .setSplitIndex(splitIndex)
                .addAllNodes(Iterables.transform(Arrays.asList(nodes), new Function<InetSocketAddress, HostAndPort>() {
                    public HostAndPort apply(final InetSocketAddress input) {
                        return HostAndPort.newBuilder().setHost(input.getHostName()).setPort(input.getPort()).build();
                    }
                }));
        addSubsetFieldsAndTermsToBuilder(intFields, stringFields, requestBuilder);
        final ImhotepRequest request = requestBuilder.build();
        final RawFTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_DISTINCT)
                .setSessionId(sessionId)
                .setField(field)
                .setIsIntField(isIntField)
                .build();
        return sendGroupStatsIteratorRequest(request, timer);
    }

    private RawFTGSIterator fileBufferedFTGSRequest(final ImhotepRequest request) {
        try {
            final Socket socket = newSocket(host, port, socketTimeout);
            final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
            final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
            try {
                sendRequest(request, is, os, host, port);
            } catch (final IOException e) {
                closeSocket(socket);
                throw e;
            }
            Path tmp = null;
            try {
                tmp = Files.createTempFile("ftgs", ".tmp");
                OutputStream out = null;
                try {
                    final long start = System.currentTimeMillis();
                    out = new LimitedBufferedOutputStream(Files.newOutputStream(tmp), tempFileSizeBytesLeft);
                    ByteStreams.copy(is, out);
                    if(log.isDebugEnabled()) {
                        log.debug("time to copy split data to file: " + (System.currentTimeMillis()
                                - start) + " ms, file length: " + Files.size(tmp));
                    }
                } catch (final Throwable t) {
                    if(t instanceof WriteLimitExceededException) {
                        throw new TempFileSizeLimitExceededException(t);
                    }
                    throw Throwables2.propagate(t, IOException.class);
                } finally {
                    if (out != null) {
                        out.close();
                    }
                }
                final BufferedInputStream bufferedInputStream = new BufferedInputStream(Files.newInputStream(tmp));
                final InputStream in = new FilterInputStream(bufferedInputStream) {
                    public void close() throws IOException {
                        bufferedInputStream.close();
                    }
                };
                return new InputStreamFTGSIterator(in, numStats);
            } finally {
                if (tmp != null) {
                    try {
                        Files.delete(tmp);
                    } catch (Exception e) {
                        log.warn("Failed to delete temp file " + tmp);
                    }
                }
                closeSocket(socket);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e); // TODO
        }
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules,
                       final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        try {
            final ImhotepResponse response =
                sendMultisplitRegroupRequest(rawRules, sessionId, errorOnCollisions);
            final int result = response.getNumGroups();
            timer.complete(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages,
                                 final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        try {
            final Iterator<GroupMultiRemapMessage> rawRuleMessagesIterator = Arrays.asList(rawRuleMessages).iterator();
            final ImhotepResponse response =
                    sendMultisplitRegroupRequestFromProtos(rawRuleMessages.length, rawRuleMessagesIterator, sessionId,
                            errorOnCollisions);
            final int result = response.getNumGroups();
            timer.complete(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int regroup(final int numRawRules,
                       final Iterator<GroupMultiRemapRule> rawRules,
                       final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        try {
            final ImhotepResponse response =
                sendMultisplitRegroupRequest(numRawRules, rawRules, sessionId,
                                             errorOnCollisions);
            final int result = response.getNumGroups();
            timer.complete(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int regroup(final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final List<GroupRemapMessage> protoRules = ImhotepClientMarshaller.marshal(rawRules);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.REGROUP)
                .setSessionId(sessionId)
                .addAllRemapRules(protoRules)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final QueryRemapMessage protoRule = ImhotepClientMarshaller.marshal(rule);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.QUERY_REGROUP)
                .setSessionId(sessionId)
                .setQueryRemapRule(protoRule)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void intOrRegroup(
            final String field,
            final long[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
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
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stringOrRegroup(
            final String field,
            final String[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
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
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void regexRegroup(
            final String field,
            final String regex,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.REGEX_REGROUP)
                .setSessionId(sessionId)
                .setField(field)
                .setRegex(regex)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void randomRegroup(
            final String field,
            final boolean isIntField,
            final String salt,
            final double p,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
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
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void randomMultiRegroup(
            final String field,
            final boolean isIntField,
            final String salt,
            final int targetGroup,
            final double[] percentages,
            final int[] resultGroups) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
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
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void randomMetricRegroup(
            final int stat,
            final String salt,
            final double p,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RANDOM_METRIC_REGROUP)
                .setSessionId(sessionId)
                .setStat(stat)
                .setSalt(salt)
                .setP(p)
                .setTargetGroup(targetGroup)
                .setNegativeGroup(negativeGroup)
                .setPositiveGroup(positiveGroup)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void randomMetricMultiRegroup(
            final int stat,
            final String salt,
            final int targetGroup,
            final double[] percentages,
            final int[] resultGroups) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RANDOM_METRIC_MULTI_REGROUP)
                .setSessionId(sessionId)
                .setStat(stat)
                .setSalt(salt)
                .setTargetGroup(targetGroup)
                .addAllPercentages(Doubles.asList(percentages))
                .addAllResultGroups(Ints.asList(resultGroups))
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int metricRegroup(final int stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_REGROUP)
                .setSessionId(sessionId)
                .setXStat(stat)
                .setXMin(min)
                .setXMax(max)
                .setXIntervalSize(intervalSize)
                .setNoGutters(noGutters)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int metricRegroup2D(
            final int xStat,
            final long xMin,
            final long xMax,
            final long xIntervalSize,
            final int yStat,
            final long yMin,
            final long yMax,
            final long yIntervalSize) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
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
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int metricFilter(
            final int stat,
            final long min,
            final long max,
            final boolean negate) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_FILTER)
                .setSessionId(sessionId)
                .setXStat(stat)
                .setXMin(min)
                .setXMax(max)
                .setNegate(negate)
                .build();
        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.APPROXIMATE_TOP_TERMS)
                .setSessionId(sessionId)
                .setField(field)
                .setIsIntField(isIntField)
                .setK(k)
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            final List<TermCount> result =
                ImhotepClientMarshaller.marshal(response.getTopTermsList());
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.PUSH_STAT)
                .setSessionId(sessionId)
                .setMetric(statName)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            numStats = response.getNumStats();
            timer.complete(request);
            return numStats;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        for (final String statName : statNames) {
            this.pushStat(statName);
        }

        return numStats;
    }

    @Override
    public int popStat() {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.POP_STAT)
                .setSessionId(sessionId)
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            numStats = response.getNumStats();
            timer.complete(request);
            return numStats;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getNumStats() {
        // TODO: really should ask the remote session just to be sure.
        return numStats;
    }

    @Override
    public int getNumGroups() {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_NUM_GROUPS)
                .setSessionId(sessionId)
                .build();
        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public long getLowerBound(final int stat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getUpperBound(final int stat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDynamicMetric(final String name) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.CREATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateDynamicMetric(final String name, final int[] deltas) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.UPDATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();

        try {
            sendRequest(request, host, port);
            timer.complete(request);
        } catch (final SocketTimeoutException e) {
            throw new RuntimeException(buildExceptionAfterSocketTimeout(e, host, port));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        final Timer timer = new Timer();
        final List<RegroupConditionMessage> conditionMessages = ImhotepClientMarshaller.marshal(conditions);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.CONDITIONAL_UPDATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .addAllConditions(conditionMessages)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();
        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        final Timer timer = new Timer();
        final List<RegroupConditionMessage> conditionMessages = ImhotepClientMarshaller.marshal(conditions);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GROUP_CONDITIONAL_UPDATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .addAllGroups(Ints.asList(groups))
                .addAllConditions(conditionMessages)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();
        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void groupQueryUpdateDynamicMetric(final String name, final int[] groups, final Query[] conditions, final int[] deltas) {
        final Timer timer = new Timer();
        final List<QueryMessage> queryMessages = new ArrayList<>();
        for (final Query q : conditions) {
            queryMessages.add(ImhotepClientMarshaller.marshal(q));
        }

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GROUP_QUERY_UPDATE_DYNAMIC_METRIC)
                .setSessionId(sessionId)
                .setDynamicMetricName(name)
                .addAllGroups(Ints.asList(groups))
                .addAllQueryMessages(queryMessages)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();
        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rebuildAndFilterIndexes(final List<String> intFields, final List<String> stringFields) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.OPTIMIZE_SESSION)
                .setSessionId(sessionId)
                .addAllIntFields(intFields)
                .addAllStringFields(stringFields)
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        internalClose(false);
    }

    private PerformanceStats internalClose(final boolean getStats) {
        if(closed) {
            return null;
        }
        final Timer timer = new Timer();
        final ImhotepRequest.Builder builder = getBuilderForType(ImhotepRequest.RequestType.CLOSE_SESSION);
        builder.setSessionId(sessionId);
        if(getStats) {
            // adding only if it's true to save bytes in request.
            builder.setReturnStatsOnClose(true);
        }

        final ImhotepRequest request = builder.build();

        PerformanceStats stats = null;
        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            if(response.hasPerformanceStats()) {
                stats = ImhotepClientMarshaller.marshal(response.getPerformanceStats());
            }
            timer.complete(request);
        } catch (final IOException e) {
            log.error("error closing session", e);
        } finally {
            closed = true;
        }
        return stats;
    }

    @Override
    public void resetGroups() {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RESET_GROUPS)
                .setSessionId(sessionId)
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getNumDocs() {
        return numDocs;
    }

    @Override
    public PerformanceStats getPerformanceStats(final boolean reset) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_PERFORMANCE_STATS)
                .setSessionId(sessionId)
                .setResetPerformanceStats(reset)
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            final PerformanceStats stats = ImhotepClientMarshaller.marshal(response.getPerformanceStats());
            timer.complete(request);
            return stats;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PerformanceStats closeAndGetPerformanceStats() {
        return internalClose(true);
    }

    public String getHost() {
        return host;
    }

    private static ImhotepRequest.Builder getBuilderForType(final ImhotepRequest.RequestType requestType) {
        return ImhotepRequest.newBuilder().setRequestType(requestType);
    }

    private static ImhotepResponse sendRequest(final ImhotepRequest request, final String host, final int port) throws IOException {
        return sendRequest(request, host, port, -1);
    }

    private static ImhotepResponse sendRequest(final ImhotepRequest request, final String host, final int port, final int socketTimeout) throws IOException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        try {
            return sendRequest(request, is, os, host, port);
        } finally {
            closeSocket(socket);
        }
    }

    // Special cased in order to save memory and only have one marshalled rule exist at a time.
    private ImhotepResponse sendMultisplitRegroupRequest(final GroupMultiRemapRule[] rules, final String sessionId, final boolean errorOnCollisions) throws IOException, ImhotepOutOfMemoryException {
        return sendMultisplitRegroupRequest(rules.length, Arrays.asList(rules).iterator(), sessionId, errorOnCollisions);
    }

    private ImhotepResponse sendMultisplitRegroupRequest(
            final int numRules,
            final Iterator<GroupMultiRemapRule> rules,
            final String sessionId,
            final boolean errorOnCollisions) throws IOException, ImhotepOutOfMemoryException {
        final Iterator<GroupMultiRemapMessage> ruleMessageIterator = Iterators.transform(rules,
                new Function<GroupMultiRemapRule, GroupMultiRemapMessage>() {
            @Nullable
            @Override
            public GroupMultiRemapMessage apply(@Nullable final GroupMultiRemapRule rule) {
                return ImhotepClientMarshaller.marshal(rule);
            }
        });
        return sendMultisplitRegroupRequestFromProtos(numRules, ruleMessageIterator, sessionId, errorOnCollisions);
    }

    private ImhotepResponse sendMultisplitRegroupRequestFromProtos(
            final int numRules,
            final Iterator<GroupMultiRemapMessage> rules,
            final String sessionId,
            final boolean errorOnCollisions) throws IOException, ImhotepOutOfMemoryException {
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
                final GroupMultiRemapMessage ruleMessage = rules.next();
                ImhotepProtobufShipping.sendProtobuf(ruleMessage, os);
            }
            return readResponseWithMemoryException(is, host, port);
        } catch (final IOException e) {
            log.error("error sending exploded multisplit regroup request to " + host + ":" + port, e);
            throw e;
        } finally {
            closeSocket(socket);
        }
    }

    private static ImhotepResponse sendRequestWithMemoryException(
            final ImhotepRequest request,
            final String host,
            final int port,
            final int socketTimeout) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse response = sendRequest(request, host, port);
        if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
            throw new ImhotepOutOfMemoryException();
        } else {
            return response;
        }
    }

    private static ImhotepResponse sendRequest(
            final ImhotepRequest request,
            final InputStream is,
            final OutputStream os,
            final String host,
            final int port) throws IOException {
        try {
            ImhotepProtobufShipping.sendProtobuf(request, os);
            final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
            if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildExceptionFromResponse(response, host, port);
            }
            return response;
        } catch (final SocketTimeoutException e) {
            throw buildExceptionAfterSocketTimeout(e, host, port);
        } catch (final IOException e) {
            final String errorMessage = "IO error with " + request.getRequestType() + " request to " + host + ":" + port;
            log.error(errorMessage, e);
            throw new IOException(errorMessage, e);
        }
    }

    private static ImhotepResponse readResponseWithMemoryException(
            final InputStream is,
            final String host,
            final int port) throws IOException, ImhotepOutOfMemoryException {
        try {
            final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
            if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildExceptionFromResponse(response, host, port);
            } else if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
                throw new ImhotepOutOfMemoryException();
            } else {
                return response;
            }
        } catch (final SocketTimeoutException e) {
            throw buildExceptionAfterSocketTimeout(e, host, port);
        }
    }

    private static IOException buildExceptionFromResponse(
            final ImhotepResponse response,
            final String host,
            final int port) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" returned error: ")
                .append(response.getExceptionStackTrace()); // stack trace string includes the type and message
        return new IOException(msg.toString());
    }

    private static IOException buildExceptionAfterSocketTimeout(
            final SocketTimeoutException e,
            final String host,
            final int port) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" socket timed out: ").append(e.getMessage());

        return new IOException(msg.toString());
    }

    private static void closeSocket(final Socket socket) {
        Closeables2.closeQuietly( socket, log );
    }

    private static Socket newSocket(final String host, final int port) throws IOException {
        return newSocket(host, port, DEFAULT_SOCKET_TIMEOUT);
    }

    public static Socket newSocket(final String host, final int port, final int timeout) throws IOException {
        final Socket socket = new Socket(host, port);
        socket.setReceiveBufferSize(65536);
        socket.setSoTimeout(timeout >= 0 ? timeout : DEFAULT_SOCKET_TIMEOUT);
        socket.setTcpNoDelay(true);
        return socket;
    }

    public InetSocketAddress getInetSocketAddress() {
        return new InetSocketAddress(host, port);
    }

    public void setNumStats(final int numStats) {
        this.numStats = numStats;
    }

    @Override
    public void writeFTGSIteratorSplit(
            final String[] intFields,
            final String[] stringFields,
            final int splitIndex,
            final int numSplits,
            final long termLimit,
            final Socket socket) {
        throw new UnsupportedOperationException("operation is unsupported!");
    }

    private final class Timer {

        final long beginTimeMillis = System.currentTimeMillis();

        void complete(final ImhotepRequest request) {
            final long elapsedTimeMillis = System.currentTimeMillis() - beginTimeMillis;
            instrumentation.fire(new SubmitRequestEvent(request, beginTimeMillis, elapsedTimeMillis));
        }

        void complete(final ImhotepRequest.RequestType requestType) {
            final long elapsedTimeMillis = System.currentTimeMillis() - beginTimeMillis;
            instrumentation.fire(new SubmitRequestEvent(requestType, beginTimeMillis, elapsedTimeMillis));
        }
    }
}
