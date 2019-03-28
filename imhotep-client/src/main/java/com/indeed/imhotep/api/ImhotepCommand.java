package com.indeed.imhotep.api;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.commands.GetGroupStats;
import com.indeed.imhotep.commands.IntOrRegroup;
import com.indeed.imhotep.commands.MetricRegroup;
import com.indeed.imhotep.commands.MultiRegroup;
import com.indeed.imhotep.commands.RandomMetricMultiRegroup;
import com.indeed.imhotep.commands.RandomMetricRegroup;
import com.indeed.imhotep.commands.RandomMultiRegroup;
import com.indeed.imhotep.commands.RandomRegroup;
import com.indeed.imhotep.commands.RegexRegroup;
import com.indeed.imhotep.commands.Regroup;
import com.indeed.imhotep.commands.RegroupUncondictional;
import com.indeed.imhotep.commands.StringOrRegroup;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Interface for each command that will be send in a Imhotep Batch Request.
 * Each command corresponds to an individual ImhotepRequest.
 */
public interface ImhotepCommand<T> extends HasSessionId {

    T combine(List<T> subResults);

    void writeToOutputStream(OutputStream os) throws IOException;

    T readResponse(InputStream is, CommandSerializationUtil serializationUtil) throws IOException, ImhotepOutOfMemoryException;

    Class<T> getResultClass();

    static List<String> getDocStatsList(final ImhotepRequest request) {
        final List<List<String>> requestStats = request.getDocStatList().stream()
                .map(docStat -> Lists.newArrayList(docStat.getStatList()))
                .collect(Collectors.toList());

        Preconditions.checkNotNull(requestStats);
        Preconditions.checkArgument(requestStats.size() == 1);
        return requestStats.get(0);
    }

    T apply(ImhotepSession session) throws ImhotepOutOfMemoryException;

    static ImhotepCommand readFromInputStream(final InputStream is) throws IOException {
        final ImhotepRequest request = ImhotepProtobufShipping.readRequest(is);
        switch (request.getRequestType()) {
            case STREAMING_GET_GROUP_STATS:
                return new GetGroupStats(getDocStatsList(request), request.getSessionId());
            case INT_OR_REGROUP:
                return new IntOrRegroup(
                        request.getField(),
                        Longs.toArray(request.getIntTermList()),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case STRING_OR_REGROUP:
                return new StringOrRegroup(
                        request.getField(),
                        request.getStringTermList(),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case METRIC_REGROUP:
                return MetricRegroup.createMetricRegroup(
                        request.getXStatDocstat().getStatList(),
                        request.getXMin(),
                        request.getXMax(),
                        request.getXIntervalSize(),
                        request.getNoGutters(),
                        request.getSessionId()
                );
            case RANDOM_METRIC_MULTI_REGROUP:
                return new RandomMetricMultiRegroup(
                        getDocStatsList(request),
                        request.getSalt(),
                        request.getTargetGroup(),
                        Doubles.toArray(request.getPercentagesList()),
                        Ints.toArray(request.getResultGroupsList()),
                        request.getSessionId()
                );
            case RANDOM_METRIC_REGROUP:
                return new RandomMetricRegroup(
                        getDocStatsList(request),
                        request.getSalt(),
                        request.getP(),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case RANDOM_MULTI_REGROUP:
                return new RandomMultiRegroup(
                        request.getField(),
                        request.getIsIntField(),
                        request.getSalt(),
                        request.getTargetGroup(),
                        Doubles.toArray(request.getPercentagesList()),
                        Ints.toArray(request.getResultGroupsList()),
                        request.getSessionId()
                );
            case RANDOM_REGROUP:
                return new RandomRegroup(
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
                        request.getField(),
                        request.getRegex(),
                        request.getTargetGroup(),
                        request.getNegativeGroup(),
                        request.getPositiveGroup(),
                        request.getSessionId()
                );
            case EXPLODED_MULTISPLIT_REGROUP:
                int numRules;
                numRules = request.getLength();
                final GroupMultiRemapRule[] rules = new GroupMultiRemapRule[numRules];
                for (int i = 0; i < numRules; i++) {
                    rules[i] = ImhotepDaemonMarshaller.marshal(ImhotepProtobufShipping.readGroupMultiRemapMessage(is));
                }
                return MultiRegroup.createMultiRegroupCommand(rules, request.getErrorOnCollisions(), request.getSessionId());
            case REGROUP:
                numRules = request.getLength();
                final GroupRemapRule[] regroupRules = new GroupRemapRule[numRules];
                for (int i = 0; i < numRules; i++) {
                    final GroupRemapMessage groupRemapMessage = ImhotepProtobufShipping.readGroupRemapMessage(is);
                    regroupRules[i] = ImhotepDaemonMarshaller.marshal(groupRemapMessage);
                }
                return Regroup.createRegroup(regroupRules, request.getSessionId());
            case REMAP_GROUPS:
                return new RegroupUncondictional(
                        Ints.toArray(request.getFromGroupsList()),
                        Ints.toArray(request.getToGroupsList()),
                        request.getFilterOutNotTargeted(),
                        request.getSessionId()
                );
            default:
                throw new IllegalArgumentException("unsupported request type in batch request: " +
                        request.getRequestType() +
                        "Batch Mode only supports Regroup and GetGroupStats Request."
                );
        }
    }

}
