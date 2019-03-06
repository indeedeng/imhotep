package com.indeed.imhotep.commands;

import com.indeed.imhotep.GroupStatsIteratorCombiner;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.RequestTools.ImhotepRequestSender;
import com.indeed.imhotep.protobuf.DocStat;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class GetGroupStats implements ImhotepCommand<GroupStatsIterator> {

    private final List<String> stats;

    public GetGroupStats(final List<String> stats) {
        this.stats = stats;
    }

    @Override
    public GroupStatsIterator combine(final GroupStatsIterator... subResults) {
        return new GroupStatsIteratorCombiner(subResults);
    }

    @Override
    public void writeToOutputStream(final OutputStream os, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepRequest request = ImhotepRequest.newBuilder().setRequestType(ImhotepRequest.RequestType.STREAMING_GET_GROUP_STATS)
                .setSessionId(imhotepRemoteSession.getSessionId())
                .addDocStat(DocStat.newBuilder().addAllStat(stats))
                .setHasStats(true)
                .build();

        final ImhotepRequestSender imhotepRequestSender = new ImhotepRequestSender.Simple(request);
        imhotepRemoteSession.sendRequestReadNoResponseFlush(imhotepRequestSender, os);
    }

    @Override
    public GroupStatsIterator readResponse(final InputStream is, final ImhotepRemoteSession imhotepRemoteSession) throws IOException {
        final ImhotepResponse response = ImhotepProtobufShipping.readResponse(is);
        final BufferedInputStream tempFileStream = imhotepRemoteSession.SaveRespsonseToFileFromStreams(is, "groupStatsIterator");
        return ImhotepProtobufShipping.readGroupStatsIterator(
                tempFileStream, response.getGroupStatSize(), false
        );
    }

    @Override
    public GroupStatsIterator[] getExecutionBuffer(int length) {
        return new GroupStatsIterator[length] ;
    }
}
