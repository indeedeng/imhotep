package com.indeed.imhotep.api;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.commands.ConsolidateGroups;
import com.indeed.imhotep.commands.DeleteGroups;
import com.indeed.imhotep.commands.GetGroupStats;
import com.indeed.imhotep.commands.GetNumGroups;
import com.indeed.imhotep.commands.IntOrRegroup;
import com.indeed.imhotep.commands.MetricRegroup;
import com.indeed.imhotep.commands.MultiRegroup;
import com.indeed.imhotep.commands.OpenSession;
import com.indeed.imhotep.commands.OpenSessionData;
import com.indeed.imhotep.commands.QueryRegroup;
import com.indeed.imhotep.commands.RandomMetricMultiRegroup;
import com.indeed.imhotep.commands.RandomMetricRegroup;
import com.indeed.imhotep.commands.RandomRegroup;
import com.indeed.imhotep.commands.RegexRegroup;
import com.indeed.imhotep.commands.ResetGroups;
import com.indeed.imhotep.commands.StringOrRegroup;
import com.indeed.imhotep.commands.TargetedMetricFilter;
import com.indeed.imhotep.commands.UnconditionalRegroup;
import com.indeed.imhotep.commands.UntargetedMetricFilter;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.protobuf.ImhotepRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Interface for each command that will be sent in a Imhotep Batch Request.
 * Each command corresponds to an individual ImhotepRequest on both client and server side.
 */
public interface ImhotepCommand<T> extends HasSessionId {

    /**
     * Merge results from all the local sessions in the server and remote session in the client.
     */
    T combine(List<T> subResults);

    /**
     * Write Imhotep Request to outputStream.
     */
    void writeToOutputStream(OutputStream os, final CommandSerializationParameters serializationParameters) throws IOException;

    /**
     * Read the response on client side.
     */
    T readResponse(InputStream is, CommandSerializationParameters serializationParameters) throws IOException, ImhotepOutOfMemoryException;

    /**
     * Get the concrete class type for creating the buffer holding local/remote session results.
     */
    Class<T> getResultClass();

    /**
     * List of Input Named Group the command is dependent on.
     */
    List<String> getInputGroups();

    /**
     * List of Output Named Groups the command will be adding / replacing.
     */
    List<String> getOutputGroups();

    /**
     * Apply the command on imhotepSession
     */
    T apply(ImhotepSession session) throws ImhotepOutOfMemoryException;

    static List<String> getSingleDocStatsList(final ImhotepRequest request) {
        final List<List<String>> requestStats = request.getDocStatList().stream()
                .map(docStat -> Lists.newArrayList(docStat.getStatList()))
                .collect(Collectors.toList());

        return Iterables.getOnlyElement(requestStats);
    }

    static ImhotepCommand readFromInputStream(final InputStream is) throws IOException {
        final ImhotepRequest request = ImhotepProtobufShipping.readRequest(is);
        final int numRules;
        switch (request.getRequestType()) {
            case STREAMING_GET_GROUP_STATS:
                return new GetGroupStats(request.getInputGroups(), getSingleDocStatsList(request), request.getSessionId());
            case INT_OR_REGROUP:
                return new IntOrRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        request.getField(),
                        Longs.toArray(request.getIntTermList()),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case STRING_OR_REGROUP:
                return new StringOrRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        request.getField(),
                        request.getStringTermList(),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case METRIC_REGROUP:
                return MetricRegroup.createMetricRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        request.getXStatDocstat().getStatList(),
                        request.getXMin(),
                        request.getXMax(),
                        request.getXIntervalSize(),
                        request.getNoGutters(),
                        request.getSessionId()
                );
            case RANDOM_METRIC_MULTI_REGROUP:
                return new RandomMetricMultiRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        getSingleDocStatsList(request),
                        request.getSalt(),
                        request.getTargetGroup(),
                        Doubles.toArray(request.getPercentagesList()),
                        Ints.toArray(request.getResultGroupsList()),
                        request.getSessionId()
                );
            case RANDOM_METRIC_REGROUP:
                return new RandomMetricRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        getSingleDocStatsList(request),
                        request.getSalt(),
                        request.getP(),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case RANDOM_REGROUP:
                return new RandomRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        request.getField(),
                        request.getIsIntField(),
                        request.getSalt(),
                        request.getP(),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case REGEX_REGROUP:
                return new RegexRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        request.getField(),
                        request.getRegex(),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case METRIC_FILTER:
                if (request.getTargetGroup() > 0) {
                    return new TargetedMetricFilter(
                            RegroupParams.fromImhotepRequest(request),
                            request.getXStatDocstat().getStatList(),
                            request.getXMin(),
                            request.getXMax(),
                            request.getTargetGroup(),
                            request.getNegativeGroup(),
                            request.getPositiveGroup(),
                            request.getSessionId()
                    );
                } else {
                    return new UntargetedMetricFilter(
                            RegroupParams.fromImhotepRequest(request),
                            request.getXStatDocstat().getStatList(),
                            request.getXMin(),
                            request.getXMax(),
                            request.getNegate(),
                            request.getSessionId()
                    );
                }
            case EXPLODED_MULTISPLIT_REGROUP:
                numRules = request.getLength();
                final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[numRules];
                for (int i = 0; i < numRules; i++) {
                    rules[i] = ImhotepDaemonMarshaller.marshal(ImhotepProtobufShipping.readGroupMultiRemapMessage(is));
                }
                return MultiRegroup.createMultiRegroupCommand(RegroupParams.fromImhotepRequest(request), rules, request.getErrorOnCollisions(), request.getSessionId());
            case QUERY_REGROUP:
                return new QueryRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        ImhotepDaemonMarshaller.marshal(request.getQueryRemapRule()),
                        request.getSessionId()
                );
            case REMAP_GROUPS:
                return new UnconditionalRegroup(
                        RegroupParams.fromImhotepRequest(request),
                        Ints.toArray(request.getFromGroupsList()),
                        Ints.toArray(request.getToGroupsList()),
                        request.getFilterOutNotTargeted(),
                        request.getSessionId()
                );
            case CONSOLIDATE_GROUPS:
                return new ConsolidateGroups(
                        request.getConsolidatedGroupsList(),
                        request.getGroupConsolidationOperation(),
                        request.getOutputGroups(),
                        request.getSessionId()
                );
            case RESET_GROUPS:
                return new ResetGroups(
                        request.getInputGroups(),
                        request.getSessionId()
                );
            case DELETE_GROUPS:
                return new DeleteGroups(
                        request.getGroupsToDeleteList(),
                        request.getSessionId()
                );
            case GET_NUM_GROUPS:
                return new GetNumGroups(
                        request.getInputGroups(),
                        request.getSessionId()
                );
            case OPEN_SESSION:
                return new OpenSession(
                        request.getSessionId(),
                        OpenSessionData.readFromImhotepRequest(request),
                        request.getShardsList(),
                        request.getClientVersion()
                );
            default:
                throw new IllegalArgumentException("unsupported request type in batch request: " +
                        request.getRequestType() +
                        "Batch Mode only supports Regroups and GetGroupStats requests."
                );
        }
    }

}
