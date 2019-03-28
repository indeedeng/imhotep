package com.indeed.imhotep.commands;

import com.indeed.imhotep.CommandSerializationUtil;
import com.indeed.imhotep.GroupStatsIteratorCombiner;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@EqualsAndHashCode
@ToString
public class GetGroupStats extends AbstractImhotepCommand<GroupStatsIterator> {

    private final List<String> stats;

    public GetGroupStats(final List<String> stats, final String sessionId) {
        super(sessionId);
        this.stats = stats;
    }

    @Override
    public ImhotepRequestSender imhotepRequestSenderInitializer() {
        final ImhotepRequest request = ImhotepRequest.newBuilder().setRequestType(ImhotepRequest.RequestType.STREAMING_GET_GROUP_STATS)
                .setSessionId(getSessionId())
                .addDocStat(DocStat.newBuilder().addAllStat(stats))
                .setHasStats(true)
                .build();

        return ImhotepRequestSender.Cached.create(request);
    }

    @Override
    public GroupStatsIterator combine(final List<GroupStatsIterator> subResults) {
        return new GroupStatsIteratorCombiner(subResults.toArray(new GroupStatsIterator[0]));
    }

    @Override
    public GroupStatsIterator apply(final ImhotepSession session) throws ImhotepOutOfMemoryException {
        return session.getGroupStatsIterator(stats);
    }

    @Override
    public GroupStatsIterator readResponse(final InputStream is, final CommandSerializationUtil serializationUtil) throws IOException, ImhotepOutOfMemoryException {
        final ImhotepResponse response = ImhotepRemoteSession.readResponseWithMemoryExceptionSessionId(is, serializationUtil.getHost(), serializationUtil.getPort(), getSessionId());
        final BufferedInputStream tempFileStream = ImhotepRemoteSession.saveResponseToFileFromStream(is, "groupStatsIterator", serializationUtil.getTempFileSizeBytesLeft(), getSessionId());
        final GroupStatsIterator groupStatsIterator = ImhotepProtobufShipping.readGroupStatsIterator(
                tempFileStream, response.getGroupStatSize(), false
        );
        return groupStatsIterator;
    }

    @Override
    public Class<GroupStatsIterator> getResultClass() {
        return GroupStatsIterator.class;
    }
}
