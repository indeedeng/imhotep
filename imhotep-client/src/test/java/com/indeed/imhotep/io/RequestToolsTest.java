package com.indeed.imhotep.io;

import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.TracingMap;
import com.indeed.imhotep.protobuf.TracingMapOnly;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class RequestToolsTest {
    @Test
    public void testSimpleRequestSenderTracingInfo() throws IOException {
        final TracingMap tracingMap = TracingMap.newBuilder()
                .addKeyValues(TracingMap.KeyValueString.newBuilder().setKey("key1").setValue("value1"))
                .addKeyValues(TracingMap.KeyValueString.newBuilder().setKey("key2").setValue("value2"))
                .build();

        final ImhotepRequest basicRequest = ImhotepRequest.newBuilder()
                .setSessionId("testsessionid")
                .setRequestType(ImhotepRequest.RequestType.OPEN_SESSION)
                .build();

        testTracingInformation(tracingMap, RequestTools.ImhotepRequestSender.Cached.create(basicRequest));
        testTracingInformation(tracingMap, new RequestTools.ImhotepRequestSender.Simple(basicRequest));
    }

    private void testTracingInformation(
            final TracingMap tracingMap,
            final RequestTools.ImhotepRequestSender sender
    ) throws IOException {
        final TracingMapOnly tracingMapOnly = TracingMapOnly.newBuilder().setTracingInfo(tracingMap).build();
        final byte[] tracingBytes = tracingMapOnly.toByteArray();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        sender.writeToStreamNoFlush(baos, tracingBytes);
        final ImhotepRequest imhotepRequest = ImhotepProtobufShipping.readRequest(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals(ImhotepRequest.RequestType.OPEN_SESSION, imhotepRequest.getRequestType());
        assertEquals("testsessionid", imhotepRequest.getSessionId());
        assertEquals(tracingMap, imhotepRequest.getTracingInfo());
    }
}