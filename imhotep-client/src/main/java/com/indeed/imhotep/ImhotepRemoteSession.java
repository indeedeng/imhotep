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
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.api.FTGAIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.FTGSParams;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.HasSessionId;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.exceptions.GenericImhotepKnownException;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.LimitedBufferedOutputStream;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.io.RequestTools.GroupMultiRemapRuleSender;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.io.TempFileSizeLimitExceededException;
import com.indeed.imhotep.io.WriteLimitExceededException;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.IntFieldAndTerms;
import com.indeed.imhotep.protobuf.MultiFTGSRequest;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.imhotep.protobuf.QueryRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.imhotep.protobuf.ShardNameNumDocsPair;
import com.indeed.imhotep.protobuf.StringFieldAndTerms;
import com.indeed.util.core.Pair;
import com.indeed.util.core.io.Closeables2;
import io.opentracing.ActiveSpan;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import it.unimi.dsi.fastutil.longs.LongIterators;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
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
import java.util.stream.Collectors;

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

    static final int DEFAULT_SOCKET_TIMEOUT = (int)TimeUnit.MINUTES.toMillis(30);

    private static final int CURRENT_CLIENT_VERSION = 2; // id to be incremented as changes to the client are done

    private final String host;
    private final int port;
    private final int socketTimeout;
    private final AtomicLong tempFileSizeBytesLeft;
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
            getProperties().put(Keys.SESSION_ID,          ImhotepRemoteSession.this.getSessionId());
            getProperties().put(Keys.REQUEST_TYPE,        requestType.toString());
            getProperties().put(Keys.BEGIN_TIME_MILLIS,   beginTimeMillis);
            getProperties().put(Keys.ELAPSED_TIME_MILLIS, elapsedTimeMillis);
            getProperties().put(Keys.SOURCE_ADDR,         ImhotepRemoteSession.this.sourceAddr);
            getProperties().put(Keys.TARGET_ADDR,         ImhotepRemoteSession.this.targetAddr);
        }
    }

    public ImhotepRemoteSession(final String host, final int port, final String sessionId,
                                final AtomicLong tempFileSizeBytesLeft) {
        this(host, port, sessionId, tempFileSizeBytesLeft, DEFAULT_SOCKET_TIMEOUT);
    }

    public ImhotepRemoteSession(final String host, final int port, final String sessionId,
                                @Nullable final AtomicLong tempFileSizeBytesLeft,
                                final int socketTimeout) {
        this(host, port, sessionId, tempFileSizeBytesLeft, socketTimeout, 0);
    }

    public ImhotepRemoteSession(final String host, final int port, final String sessionId,
                                @Nullable final AtomicLong tempFileSizeBytesLeft,
                                final int socketTimeout, final long numDocs) {
        super(sessionId);
        this.host = host;
        this.port = port;
        this.socketTimeout = socketTimeout;
        this.tempFileSizeBytesLeft = tempFileSizeBytesLeft;
        this.numDocs = numDocs;

        String tmpAddr;
        try {
            tmpAddr = InetAddress.getLocalHost().toString();
        }
        catch (final Exception ex) {
            tmpAddr = "";
            log.warn("[" + getSessionId() + "] Cannot initialize sourceAddr", ex);
        }
        this.sourceAddr = tmpAddr;

        try {
            tmpAddr = InetAddress.getByName(host).toString();
        }
        catch (final Exception ex) {
            tmpAddr = host;
            log.warn("[" + getSessionId() + "] Cannot initialize targetAddr", ex);
        }
        this.targetAddr = tmpAddr;
    }

    public static ImhotepStatusDump getStatusDump(final String host, final int port) throws IOException {
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_STATUS_DUMP)
                .build();

        final ImhotepResponse response = sendRequest(createSender(request), host, port);

        return ImhotepStatusDump.fromProto(response.getStatusDump());
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<Shard> shards, @Nullable final String sessionId, final long sessionTimeout) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, DEFAULT_MERGE_THREAD_LIMIT, getUsername(), false, -1, sessionId, -1, null, sessionTimeout);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<Shard> shards,
                                                   final int mergeThreadLimit, @Nullable final String sessionId, final long sessionTimeout) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, mergeThreadLimit, getUsername(), false, -1, sessionId, -1, null, sessionTimeout);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<Shard> shards,
                                                   final int mergeThreadLimit, final String username,
                                                   final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                                   @Nullable final String sessionId, final long tempFileSizeLimit,
                                                   @Nullable final AtomicLong tempFileSizeBytesLeft,
                                                   final long sessionTimeout) throws ImhotepOutOfMemoryException, IOException {
        return openSession(host, port, dataset, shards, mergeThreadLimit, username, optimizeGroupZeroLookups, socketTimeout, sessionId, tempFileSizeLimit, tempFileSizeBytesLeft, sessionTimeout, 0);
    }

    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<Shard> shards,
                                                   final int mergeThreadLimit, final String username,
                                                   final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                                   @Nullable final String sessionId, final long tempFileSizeLimit,
                                                   @Nullable final AtomicLong tempFileSizeBytesLeft,
                                                   final long sessionTimeout, final long numDocs) throws ImhotepOutOfMemoryException, IOException {

        return openSession(host, port, dataset, shards, mergeThreadLimit, username, "", optimizeGroupZeroLookups, socketTimeout, sessionId, tempFileSizeLimit, tempFileSizeBytesLeft, sessionTimeout, false, numDocs);
    }


    public static ImhotepRemoteSession openSession(final String host, final int port, final String dataset, final List<Shard> shards,
                                                   final int mergeThreadLimit, final String username, final String clientName,
                                                   final boolean optimizeGroupZeroLookups, final int socketTimeout,
                                                   @Nullable String sessionId, final long tempFileSizeLimit,
                                                   @Nullable final AtomicLong tempFileSizeBytesLeft,
                                                   final long sessionTimeout,
                                                   boolean allowSessionForwarding, final long numDocs) throws ImhotepOutOfMemoryException, IOException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final Tracer tracer = GlobalTracer.get();
        try (final ActiveSpan activeSpan = tracer.buildSpan("OPEN_SESSION").withTag("sessionid", sessionId).withTag("dataset", dataset).withTag("host", host + ":" + port).startActive()) {
            log.trace("sending open request to "+host+":"+port+" for shards "+shards);
            final ImhotepRequest openSessionRequest = getBuilderForType(ImhotepRequest.RequestType.OPEN_SESSION)
                    .setUsername(username)
                    .setClientName(clientName)
                    .setDataset(dataset)
                    .setMergeThreadLimit(mergeThreadLimit)
                    .addAllShards(shards.stream().map(shard -> ShardNameNumDocsPair.newBuilder().setShardName(shard.getFileName()).setNumDocs(shard.numDocs).build()).collect(Collectors.toList()))
                    .setOptimizeGroupZeroLookups(optimizeGroupZeroLookups)
                    .setClientVersion(CURRENT_CLIENT_VERSION)
                    .setSessionId(sessionId == null ? "" : sessionId)
                    .setTempFileSizeLimit(tempFileSizeLimit)
                    .setSessionTimeout(sessionTimeout)
                    .setAllowSessionForwarding(allowSessionForwarding)
                    .build();
            try {
                ImhotepProtobufShipping.sendProtobuf(openSessionRequest, os);

                log.trace("waiting for confirmation from "+host+":"+port);
                final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
                if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
                    throw new ImhotepOutOfMemoryException(createMessageWithSessionId(
                            "OutOfMemory error when creating session",
                            (sessionId == null) ? "" : sessionId));
                }
                if (response.getResponseCode() == ImhotepResponse.ResponseCode.KNOWN_ERROR) {
                    throw buildImhotepKnownExceptionFromResponse(response, host, port, sessionId);
                }
                if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                    throw buildIOExceptionFromResponse(response, host, port, sessionId);
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
                return new ImhotepRemoteSession(host, actualPort, sessionId, tempFileSizeBytesLeft, socketTimeout, numDocs);
            } catch (final SocketTimeoutException e) {
                throw buildExceptionAfterSocketTimeout(e, host, port, sessionId);
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
                .setSessionId(getSessionId())
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            final long result = response.getTotalDocFreq();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public long[] getGroupStats(final List<String> stat) throws ImhotepOutOfMemoryException {
        try (final GroupStatsIterator reader = getGroupStatsIterator(stat)) {
            return LongIterators.unwrap(reader, reader.getNumGroups());
        } catch(final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public GroupStatsIterator getGroupStatsIterator(final List<String> stat) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.STREAMING_GET_GROUP_STATS)
                .setSessionId(getSessionId())
                .addDocStat(DocStat.newBuilder().addAllStat(stat))
                .setHasStats(true)
                .build();
        return sendGroupStatsIteratorRequest(request, timer);
    }

    @Override
    public FTGSIterator getFTGSIterator(final FTGSParams params) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.GET_FTGS_ITERATOR)
                .setSessionId(getSessionId())
                .addAllIntFields(Arrays.asList(params.intFields))
                .addAllStringFields(Arrays.asList(params.stringFields))
                .setTermLimit(params.termLimit)
                .setSortStat(params.sortStat)
                .setSortedFTGS(params.sorted);

        if (params.stats != null) {
            requestBuilder
                    .addAllDocStat(params.stats.stream().map(x -> DocStat.newBuilder().addAllStat(x).build()).collect(Collectors.toList()))
                    .setHasStats(true);
        }

        final ImhotepRequest request = requestBuilder.build();

        final FTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    FTGAIterator multiFTGS(final MultiFTGSRequest proto) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.MERGE_MULTI_FTGS_SPLIT)
                .setMultiFtgsRequest(proto)
                .build();

        try {
            final Pair<ImhotepResponse, InputStream> responseAndFile = sendRequestAndSaveResponseToFile(request, "ftgs");
            final int numStats = responseAndFile.getFirst().getNumStats();
            final int numGroups = responseAndFile.getFirst().getNumGroups();
            timer.complete(request);
            return new InputStreamFTGAIterator(responseAndFile.getSecond(), null, numStats, numGroups);
        } catch (final IOException e) {
            throw new RuntimeException(e); // TODO
        }
    }

    GroupStatsIterator aggregateDistinct(final MultiFTGSRequest proto) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.MERGE_MULTI_DISTINCT_SPLIT)
                .setMultiFtgsRequest(proto)
                .build();
        return sendGroupStatsIteratorRequest(request, timer);
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, @Nullable final List<List<String>> stats) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.GET_SUBSET_FTGS_ITERATOR)
                .setSessionId(getSessionId());
        addSubsetFieldsAndTermsToBuilder(intFields, stringFields, requestBuilder);
        if (stats != null) {
            for (final List<String> stat : stats) {
                requestBuilder.addDocStat(DocStat.newBuilder().addAllStat(stat));
            }
            requestBuilder.setHasStats(true);
        }
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

    public FTGSIterator getFTGSIteratorSplit(final String[] intFields, final String[] stringFields, @Nullable final List<List<String>> stats, final int splitIndex, final int numSplits, final long termLimit) throws ImhotepOutOfMemoryException {
        checkSplitParams(splitIndex, numSplits);
        // TODO: disable timer to reduce logrepo logging volume of SubmitRequestEvent?
        final Timer timer = new Timer();
        final ImhotepRequest.Builder builder = getBuilderForType(ImhotepRequest.RequestType.GET_FTGS_SPLIT)
                .setSessionId(getSessionId())
                .addAllIntFields(Arrays.asList(intFields))
                .addAllStringFields(Arrays.asList(stringFields))
                .setSplitIndex(splitIndex)
                .setNumSplits(numSplits)
                .setTermLimit(termLimit)
                .setSortStat(-1) // never top terms
                .setSortedFTGS(true); // always sorted
        if (stats != null) {
            builder
                .addAllDocStat(stats.stream().map(x -> DocStat.newBuilder().addAllStat(x).build()).collect(Collectors.toList()))
                .setHasStats(true);
        }
        final ImhotepRequest request = builder.build();
        final FTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    public FTGSIterator getSubsetFTGSIteratorSplit(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields,
            @Nullable final List<List<String>> stats,
            final int splitIndex,
            final int numSplits) throws ImhotepOutOfMemoryException {
        checkSplitParams(splitIndex, numSplits);
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.GET_SUBSET_FTGS_SPLIT)
                .setSessionId(getSessionId())
                .setSplitIndex(splitIndex)
                .setNumSplits(numSplits);
        if (stats != null) {
            requestBuilder
                    .addAllDocStat(stats.stream().map(x -> DocStat.newBuilder().addAllStat(x).build()).collect(Collectors.toList()))
                    .setHasStats(true);
        }
        addSubsetFieldsAndTermsToBuilder(intFields, stringFields, requestBuilder);
        final ImhotepRequest request = requestBuilder.build();
        final FTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    public FTGSIterator mergeFTGSSplit(
            final FTGSParams params,
            final InetSocketAddress[] nodes,
            final int splitIndex) throws ImhotepOutOfMemoryException {
        checkSplitParams(splitIndex, nodes.length);
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.MERGE_FTGS_SPLIT)
                .setSessionId(getSessionId())
                .addAllIntFields(Arrays.asList(params.intFields))
                .addAllStringFields(Arrays.asList(params.stringFields))
                .setSplitIndex(splitIndex)
                .setTermLimit(params.termLimit)
                .setSortStat(params.sortStat)
                .setSortedFTGS(params.sorted)
                .addAllNodes(Iterables.transform(Arrays.asList(nodes), new Function<InetSocketAddress, HostAndPort>() {
                    public HostAndPort apply(final InetSocketAddress input) {
                        return HostAndPort.newBuilder().setHost(input.getHostName()).setPort(input.getPort()).build();
                    }
                }));

        if (params.stats != null) {
            requestBuilder
                    .addAllDocStat(params.stats.stream().map(x -> DocStat.newBuilder().addAllStat(x).build()).collect(Collectors.toList()))
                    .setHasStats(true);
        }

        final ImhotepRequest request = requestBuilder.build();
        final FTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    public GroupStatsIterator mergeDistinctSplit(final String field,
                                                 final boolean isIntField,
                                                 final InetSocketAddress[] nodes,
                                                 final int splitIndex) {
        checkSplitParams(splitIndex, nodes.length);
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.MERGE_DISTINCT_SPLIT)
                .setSessionId(getSessionId())
                .setField(field)
                .setIsIntField(isIntField)
                .setSplitIndex(splitIndex)
                .addAllNodes(Iterables.transform(Arrays.asList(nodes), input -> HostAndPort.newBuilder().setHost(input.getHostName()).setPort(input.getPort()).build()))
                .build();
        try {
            return sendGroupStatsIteratorRequest(request, timer);
        } catch (final ImhotepOutOfMemoryException e) {
            throw new RuntimeException("mergeDistinct expected to use no stats but threw IOOME", e);
        }
    }

    private GroupStatsIterator sendGroupStatsIteratorRequest(final ImhotepRequest request, final Timer timer) throws ImhotepOutOfMemoryException {
        try {
            final Pair<ImhotepResponse, InputStream> responceAndFile = sendRequestAndSaveResponseToFile(request, "groupStatsIterator");
            timer.complete(request);
            return ImhotepProtobufShipping.readGroupStatsIterator(
                    responceAndFile.getSecond(),
                    responceAndFile.getFirst().getGroupStatSize(),
                    false);
        } catch(final IOException e) {
            throw newRuntimeException(e);
        }
    }

    public FTGSIterator mergeSubsetFTGSSplit(
            final Map<String, long[]> intFields,
            final Map<String, String[]> stringFields,
            @Nullable final List<List<String>> stats,
            final InetSocketAddress[] nodes,
            final int splitIndex) throws ImhotepOutOfMemoryException {
        checkSplitParams(splitIndex, nodes.length);
        final Timer timer = new Timer();
        final ImhotepRequest.Builder requestBuilder = getBuilderForType(ImhotepRequest.RequestType.MERGE_SUBSET_FTGS_SPLIT)
                .setSessionId(getSessionId())
                .setSplitIndex(splitIndex)
                .addAllNodes(Iterables.transform(Arrays.asList(nodes), new Function<InetSocketAddress, HostAndPort>() {
                    public HostAndPort apply(final InetSocketAddress input) {
                        return HostAndPort.newBuilder().setHost(input.getHostName()).setPort(input.getPort()).build();
                    }
                }));
        if (stats != null) {
            for (final List<String> stat : stats) {
                requestBuilder.addDocStatBuilder().addAllStat(stat);
            }
            requestBuilder.setHasStats(true);
        }
        addSubsetFieldsAndTermsToBuilder(intFields, stringFields, requestBuilder);
        final ImhotepRequest request = requestBuilder.build();
        final FTGSIterator result = fileBufferedFTGSRequest(request);
        timer.complete(request);
        return result;
    }

    @Override
    public GroupStatsIterator getDistinct(final String field, final boolean isIntField) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GET_DISTINCT)
                .setSessionId(getSessionId())
                .setField(field)
                .setIsIntField(isIntField)
                .build();
        try {
            return sendGroupStatsIteratorRequest(request, timer);
        } catch (final ImhotepOutOfMemoryException e) {
            throw new RuntimeException("getDistinct should use no memory but threw ImhotepOutOfMemoryException", e);
        }
    }

    private FTGSIterator fileBufferedFTGSRequest(final ImhotepRequest request) throws ImhotepOutOfMemoryException {
        try {
            final Pair<ImhotepResponse, InputStream> responseAndFile = sendRequestAndSaveResponseToFile(request, "ftgs");
            final int numStats = responseAndFile.getFirst().getNumStats();
            final int numGroups = responseAndFile.getFirst().getNumGroups();
            return new InputStreamFTGSIterator(responseAndFile.getSecond(), null, numStats, numGroups);
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    private Pair<ImhotepResponse, InputStream> sendRequestAndSaveResponseToFile(
            final ImhotepRequest request,
            final String tempFilePrefix) throws IOException, ImhotepOutOfMemoryException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final ImhotepResponse response;
        Path tmp = null;
        try {
            response = checkMemoryException(getSessionId(), sendRequest(createSender(request), is, os, host, port));
            tmp = Files.createTempFile(tempFilePrefix, ".tmp");
            final long start = System.currentTimeMillis();
            try (final OutputStream out = new LimitedBufferedOutputStream(Files.newOutputStream(tmp), tempFileSizeBytesLeft)) {
                ByteStreams.copy(is, out);
            } catch (final WriteLimitExceededException t) {
                final String messageWithSessionId = createMessageWithSessionId(
                        TempFileSizeLimitExceededException.MESSAGE, getSessionId());
                throw new TempFileSizeLimitExceededException(messageWithSessionId, t);
            } finally {
                if(log.isDebugEnabled()) {
                    log.debug("[" + getSessionId() + "] time to copy split data to file: " + (System.currentTimeMillis()
                            - start) + " ms, file length: " + Files.size(tmp));
                }
            }
            final BufferedInputStream bufferedInputStream = new BufferedInputStream(Files.newInputStream(tmp));
            return new Pair<>(response, bufferedInputStream);
        } finally {
            if (tmp != null) {
                try {
                    Files.delete(tmp);
                } catch (final Exception e) {
                    log.warn("[" + getSessionId() + "] Failed to delete temp file " + tmp);
                }
            }
            closeSocket(socket);
        }
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules,
                       final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final GroupMultiRemapRuleSender ruleSender =
                GroupMultiRemapRuleSender.createFromRules(Arrays.asList(rawRules).iterator(), false);
        return regroupWithSender(ruleSender, errorOnCollisions, timer);
    }

    @Override
    public int regroupWithProtos(final GroupMultiRemapMessage[] rawRuleMessages,
                                 final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final RequestTools.GroupMultiRemapRuleSender ruleSender =
                GroupMultiRemapRuleSender.createFromMessages(Arrays.asList(rawRuleMessages).iterator(), false);
        return regroupWithSender(ruleSender, errorOnCollisions, timer);
    }

    public int regroupWithSender(final GroupMultiRemapRuleSender ruleSender,
                                 final boolean errorOnCollisions,
                                 final Timer timer) throws ImhotepOutOfMemoryException {
        try {
            final int result = sendMultisplitRegroupRequest(ruleSender, errorOnCollisions);
            timer.complete(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP);
            return result;
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public int regroup(final int numRawRules,
                       final Iterator<GroupMultiRemapRule> rawRules,
                       final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final GroupMultiRemapRuleSender ruleSender = GroupMultiRemapRuleSender.createFromRules(rawRules, false);
        return regroupWithSender(ruleSender, errorOnCollisions, timer);
    }

    @Override
    public int regroup(final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final List<GroupRemapMessage> protoRules = ImhotepClientMarshaller.marshal(rawRules);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.REGROUP)
                .setSessionId(getSessionId())
                .addAllRemapRules(protoRules)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final QueryRemapMessage protoRule = ImhotepClientMarshaller.marshal(rule);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.QUERY_REGROUP)
                .setSessionId(getSessionId())
                .setQueryRemapRule(protoRule)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request);
            return result;
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public void intOrRegroup(
            final String field,
            final long[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequestSender request = createSender(buildIntOrRegroupRequest(field, terms, targetGroup, negativeGroup, positiveGroup));
        sendVoidRequest(request);
    }

    protected void sendVoidRequest(final ImhotepRequestSender request) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
            timer.complete(request.getRequestType());
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    protected ImhotepRequest buildIntOrRegroupRequest(String field, long[] terms, int targetGroup, int negativeGroup, int positiveGroup) {
        return getBuilderForType(ImhotepRequest.RequestType.INT_OR_REGROUP)
                    .setSessionId(getSessionId())
                    .setField(field)
                    .addAllIntTerm(Longs.asList(terms))
                    .setTargetGroup(targetGroup)
                    .setNegativeGroup(negativeGroup)
                    .setPositiveGroup(positiveGroup)
                    .build();
    }

    @Override
    public void stringOrRegroup(
            final String field,
            final String[] terms,
            final int targetGroup,
            final int negativeGroup,
            final int positiveGroup) throws ImhotepOutOfMemoryException {
        final ImhotepRequestSender request = createSender(buildStringOrRegroupRequest(field, terms, targetGroup, negativeGroup, positiveGroup));
        sendVoidRequest(request);
    }

    protected ImhotepRequest buildStringOrRegroupRequest(String field, String[] terms, int targetGroup, int negativeGroup, int positiveGroup) {
        return getBuilderForType(ImhotepRequest.RequestType.STRING_OR_REGROUP)
                    .setSessionId(getSessionId())
                    .setField(field)
                    .addAllStringTerm(Arrays.asList(terms))
                    .setTargetGroup(targetGroup)
                    .setNegativeGroup(negativeGroup)
                    .setPositiveGroup(positiveGroup)
                    .build();
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
                .setSessionId(getSessionId())
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
            throw newRuntimeException(e);
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
                .setSessionId(getSessionId())
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
            throw newRuntimeException(e);
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
                .setSessionId(getSessionId())
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
            throw newRuntimeException(e);
        }
    }

    @Override
    public void randomMetricRegroup(final List<String> stat, final String salt, final double p, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RANDOM_METRIC_REGROUP)
                .setSessionId(getSessionId())
                .addDocStat(DocStat.newBuilder().addAllStat(stat))
                .setHasStats(true)
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
            throw newRuntimeException(e);
        }
    }

    @Override
    public void randomMetricMultiRegroup(final List<String> stat, final String salt, final int targetGroup, final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RANDOM_METRIC_MULTI_REGROUP)
                .setSessionId(getSessionId())
                .addDocStat(DocStat.newBuilder().addAllStat(stat))
                .setHasStats(true)
                .setSalt(salt)
                .setTargetGroup(targetGroup)
                .addAllPercentages(Doubles.asList(percentages))
                .addAllResultGroups(Ints.asList(resultGroups))
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public int metricRegroup(final List<String> stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_REGROUP)
                .setSessionId(getSessionId())
                .setXStatDocstat(DocStat.newBuilder().addAllStat(stat))
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
            throw newRuntimeException(e);
        }
    }

    @Override
    public int regroup(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        final ImhotepRequestSender request = createSender(buildGroupRemapRequest(fromGroups, toGroups, filterOutNotTargeted));
        return sendRegroupRequest(request);
    }

    public ImhotepRequest buildGroupRemapRequest(final int[] fromGroups, final int[] toGroups, final boolean filterOutNotTargeted) throws ImhotepOutOfMemoryException {
        return getBuilderForType(ImhotepRequest.RequestType.REMAP_GROUPS)
                .setSessionId(getSessionId())
                .addAllFromGroups(Ints.asList(fromGroups))
                .addAllToGroups(Ints.asList(toGroups))
                .setFilterOutNotTargeted(filterOutNotTargeted)
                .build();
    }

    public int sendRegroupRequest(final ImhotepRequestSender request) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            final int result = response.getNumGroups();
            timer.complete(request.getRequestType());
            return result;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int metricFilter(final List<String> stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_FILTER)
                .setSessionId(getSessionId())
                .setXStatDocstat(DocStat.newBuilder().addAllStat(stat))
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
    public int metricFilter(final List<String> stat, final long min, final long max, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.METRIC_FILTER)
                .setSessionId(getSessionId())
                .setXStatDocstat(DocStat.newBuilder().addAllStat(stat))
                .setXMin(min)
                .setXMax(max)
                .setTargetGroup(targetGroup)
                .setPositiveGroup(positiveGroup)
                .setNegativeGroup(negativeGroup)
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
                .setSessionId(getSessionId())
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
            throw newRuntimeException(e);
        }
    }

    @Override
    public int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.PUSH_STAT)
                .setSessionId(getSessionId())
                .setMetric(statName)
                .build();

        try {
            final ImhotepResponse response = sendRequestWithMemoryException(request, host, port, socketTimeout);
            numStats = response.getNumStats();
            timer.complete(request);
            return numStats;
        } catch (final IOException e) {
            throw newRuntimeException(e);
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
                .setSessionId(getSessionId())
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            numStats = response.getNumStats();
            timer.complete(request);
            return numStats;
        } catch (final IOException e) {
            throw newRuntimeException(e);
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
                .setSessionId(getSessionId())
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
                .setSessionId(getSessionId())
                .setDynamicMetricName(name)
                .build();

        try {
            sendRequestWithMemoryException(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public void updateDynamicMetric(final String name, final int[] deltas) {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.UPDATE_DYNAMIC_METRIC)
                .setSessionId(getSessionId())
                .setDynamicMetricName(name)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final SocketTimeoutException e) {
            throw newRuntimeException(buildExceptionAfterSocketTimeout(e, host, port, null));
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        final Timer timer = new Timer();
        final List<RegroupConditionMessage> conditionMessages = ImhotepClientMarshaller.marshal(conditions);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.CONDITIONAL_UPDATE_DYNAMIC_METRIC)
                .setSessionId(getSessionId())
                .setDynamicMetricName(name)
                .addAllConditions(conditionMessages)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();
        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        final Timer timer = new Timer();
        final List<RegroupConditionMessage> conditionMessages = ImhotepClientMarshaller.marshal(conditions);

        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.GROUP_CONDITIONAL_UPDATE_DYNAMIC_METRIC)
                .setSessionId(getSessionId())
                .setDynamicMetricName(name)
                .addAllGroups(Ints.asList(groups))
                .addAllConditions(conditionMessages)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();
        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw newRuntimeException(e);
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
                .setSessionId(getSessionId())
                .setDynamicMetricName(name)
                .addAllGroups(Ints.asList(groups))
                .addAllQueryMessages(queryMessages)
                .addAllDynamicMetricDeltas(Ints.asList(deltas))
                .build();
        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw newRuntimeException(e);
        }
    }

    @Override
    public void rebuildAndFilterIndexes(final List<String> intFields, final List<String> stringFields) throws ImhotepOutOfMemoryException {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.OPTIMIZE_SESSION)
                .setSessionId(getSessionId())
                .addAllIntFields(intFields)
                .addAllStringFields(stringFields)
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw newRuntimeException(e);
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
        builder.setSessionId(getSessionId());
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
            log.error("[" + getSessionId() + "] error closing session", e);
        } finally {
            closed = true;
        }
        return stats;
    }

    @Override
    public void resetGroups() {
        final Timer timer = new Timer();
        final ImhotepRequest request = getBuilderForType(ImhotepRequest.RequestType.RESET_GROUPS)
                .setSessionId(getSessionId())
                .build();

        try {
            sendRequest(request, host, port, socketTimeout);
            timer.complete(request);
        } catch (final IOException e) {
            throw newRuntimeException(e);
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
                .setSessionId(getSessionId())
                .setResetPerformanceStats(reset)
                .build();

        try {
            final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
            final PerformanceStats stats = ImhotepClientMarshaller.marshal(response.getPerformanceStats());
            timer.complete(request);
            return stats;
        } catch (final IOException e) {
            throw newRuntimeException(e);
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

    private static ImhotepResponse sendRequest(final ImhotepRequestSender request, final String host, final int port) throws IOException {
        return sendRequest(request, host, port, -1);
    }

    private static ImhotepResponse sendRequest(final ImhotepRequestSender request, final String host, final int port, final int socketTimeout) throws IOException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        try {
            return sendRequest(request, is, os, host, port);
        } finally {
            closeSocket(socket);
        }
    }

    private static ImhotepResponse sendRequest(final ImhotepRequest request, final String host, final int port, final int socketTimeout) throws IOException {
        final Socket socket = newSocket(host, port, socketTimeout);
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final ImhotepRequestSender requestSender = createSender(request);
        try {
            return sendRequest(requestSender, is, os, host, port);
        } finally {
            closeSocket(socket);
        }
    }

    private int sendMultisplitRegroupRequest(
            final GroupMultiRemapRuleSender rulesSender,
            final boolean errorOnCollisions) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepRequest initialRequest = getBuilderForType(ImhotepRequest.RequestType.EXPLODED_MULTISPLIT_REGROUP)
                .setLength(rulesSender.getRulesCount())
                .setSessionId(getSessionId())
                .setErrorOnCollisions(errorOnCollisions)
                .build();

        final Socket socket = newSocket(host, port, socketTimeout);
        final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
        final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
        final Tracer tracer = GlobalTracer.get();
        final String sessionId = initialRequest.getSessionId();
        try (final ActiveSpan activeSpan = tracer.buildSpan(initialRequest.getRequestType().name()).withTag("sessionid", sessionId).withTag("host", host + ":" + port).startActive()) {
            ImhotepProtobufShipping.sendProtobufNoFlush(initialRequest, os);
            rulesSender.writeToStreamNoFlush(os);
            os.flush();
            final ImhotepResponse response = readResponseWithMemoryException(is, host, port);
            return response.getNumGroups();
        } catch (final IOException e) {
            log.error("[" + getSessionId() + "] error sending exploded multisplit regroup request to " + host + ":" + port, e);
            throw e;
        } finally {
            closeSocket(socket);
        }
    }

    private static ImhotepResponse sendRequestWithMemoryException(
            final ImhotepRequestSender request,
            final String host,
            final int port,
            final int socketTimeout) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
        if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
            throw new ImhotepOutOfMemoryException();
        } else {
            return response;
        }
    }

    private static ImhotepResponse checkMemoryException(@Nullable final String sessionId, final ImhotepResponse response) throws ImhotepOutOfMemoryException {
        if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
            throw new ImhotepOutOfMemoryException(createMessageWithSessionId("OutOfMemory error", sessionId));
        } else {
            return response;
        }
    }

    private static ImhotepResponse sendRequestWithMemoryException(
            final ImhotepRequest request,
            final String host,
            final int port,
            final int socketTimeout) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse response = sendRequest(request, host, port, socketTimeout);
        return checkMemoryException(request.getSessionId(), response);
    }

    private static ImhotepResponse sendRequest(
            final ImhotepRequestSender request,
            final InputStream is,
            final OutputStream os,
            final String host,
            final int port) throws IOException {
        final Tracer tracer = GlobalTracer.get();
        final String sessionId = request.getSessionId();
        try (final ActiveSpan activeSpan = tracer.buildSpan(request.getRequestType().name()).withTag("sessionid", sessionId).withTag("host", host + ":" + port).startActive()) {
            request.writeToStreamNoFlush(os);
            os.flush();
            final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
            if (response.getResponseCode() == ImhotepResponse.ResponseCode.KNOWN_ERROR) {
                throw buildImhotepKnownExceptionFromResponse(response, host, port, sessionId);
            }
            if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildIOExceptionFromResponse(response, host, port, sessionId);
            }
            return response;
        } catch (final SocketTimeoutException e) {
            throw buildExceptionAfterSocketTimeout(e, host, port, sessionId);
        } catch (final IOException e) {
            String errorMessage = "IO error with " + request.getRequestType() + " request to " + host + ":" + port;
            if (sessionId != null) {
                errorMessage = createMessageWithSessionId(errorMessage, sessionId);
            }
            log.error(errorMessage, e);
            throw new IOException(errorMessage, e);
        }
    }

    private ImhotepResponse readResponseWithMemoryException(
            final InputStream is,
            final String host,
            final int port) throws IOException, ImhotepOutOfMemoryException {
        try {
            final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
            if (response.getResponseCode() == ImhotepResponse.ResponseCode.KNOWN_ERROR) {
                throw buildImhotepKnownExceptionFromResponse(response, host, port, getSessionId());
            } else if (response.getResponseCode() == ImhotepResponse.ResponseCode.OTHER_ERROR) {
                throw buildIOExceptionFromResponse(response, host, port, getSessionId());
            } else if (response.getResponseCode() == ImhotepResponse.ResponseCode.OUT_OF_MEMORY) {
                throw newImhotepOutOfMemoryException();
            } else {
                return response;
            }
        } catch (final SocketTimeoutException e) {
            throw buildExceptionAfterSocketTimeout(e, host, port, getSessionId());
        }
    }

    private static IOException buildIOExceptionFromResponse(
            final ImhotepResponse response,
            final String host,
            final int port,
            @Nullable final String sessionId) {
        final String msg = buildExceptionMessage(response, host, port, sessionId);
        return new IOException(msg);
    }

    private static ImhotepKnownException buildImhotepKnownExceptionFromResponse(
            final ImhotepResponse response,
            final String host,
            final int port,
            @Nullable final String sessionId) {
        final String msg = buildExceptionMessage(response, host, port, sessionId);
        return new GenericImhotepKnownException(msg);
    }

    private static String buildExceptionMessage(final ImhotepResponse response, final String host, final int port, @Nullable final String sessionId) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" returned error: ")
                .append(response.getExceptionStackTrace()); // stack trace string includes the type and message
        if (sessionId != null) {
            msg.append(" sessionId :").append(sessionId);
        }
        return msg.toString();
    }

    private static IOException buildExceptionAfterSocketTimeout(
            final SocketTimeoutException e,
            final String host,
            final int port,
            @Nullable final String sessionId) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" socket timed out: ").append(e.getMessage());
        if (sessionId != null) {
            msg.append(" sessionId: ").append(sessionId);
        }

        return new IOException(msg.toString());
    }

    private static void closeSocket(final Socket socket) {
        Closeables2.closeQuietly( socket, log );
    }

    private static ImhotepRequestSender createSender(final ImhotepRequest request) {
        return new RequestTools.ImhotepRequestSender.Simple(request);
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

    public Timer newTimer() {
        return new Timer();
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
