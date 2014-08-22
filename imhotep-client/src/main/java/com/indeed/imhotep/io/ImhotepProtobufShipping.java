package com.indeed.imhotep.io;

import com.google.common.io.ByteStreams;
import com.google.protobuf.Message;
import com.indeed.imhotep.frontend.protobuf.ImhotepFrontendRequest;
import com.indeed.imhotep.frontend.protobuf.ImhotepFrontendResponse;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author jsgroth
 * 
 * static utility methods for sending and receiving imhotep protobufs
 */
public final class ImhotepProtobufShipping {
    private ImhotepProtobufShipping() {}

    public static void sendProtobuf(Message request, OutputStream os) throws IOException {
        os.write(Bytes.intToBytes(request.getSerializedSize()));
        request.writeTo(os);
        os.flush();
    }

    public static ImhotepRequest readRequest(InputStream is) throws IOException {
        return ImhotepRequest.parseFrom(readPayloadStream(is));
    }

    public static GroupMultiRemapMessage readGroupMultiRemapMessage(InputStream is) throws IOException {
        return GroupMultiRemapMessage.parseFrom(readPayloadStream(is));
    }

    public static GroupRemapMessage readGroupRemapMessage(InputStream is) throws IOException {
        return GroupRemapMessage.parseFrom(readPayloadStream(is));
    }

    public static ImhotepResponse readResponse(InputStream is) throws IOException {
        return ImhotepResponse.parseFrom(readPayloadStream(is));
    }

    public static ImhotepFrontendRequest readFrontendRequest(InputStream is) throws IOException {
        return ImhotepFrontendRequest.parseFrom(readPayloadStream(is));
    }

    public static ImhotepFrontendResponse readFrontendResponse(InputStream is) throws IOException {
        return ImhotepFrontendResponse.parseFrom(readPayloadStream(is));
    }

    private static InputStream readPayloadStream(InputStream is) throws IOException {
        final byte[] payloadLengthBytes = new byte[4];
        ByteStreams.readFully(is, payloadLengthBytes);
        final int payloadLength = Bytes.bytesToInt(payloadLengthBytes);

        return ByteStreams.limit(is, payloadLength);
    }
}
