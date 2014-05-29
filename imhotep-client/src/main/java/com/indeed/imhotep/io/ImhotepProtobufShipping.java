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
        final byte[] payload = request.toByteArray();
        final byte[] payloadLengthBytes = Bytes.intToBytes(payload.length);

        os.write(payloadLengthBytes);
        os.write(payload);
        os.flush();
    }

    public static ImhotepRequest readRequest(InputStream is) throws IOException {
        final byte[] requestBytes = readPayload(is);
        return ImhotepRequest.parseFrom(requestBytes);
    }

    public static GroupMultiRemapMessage readGroupMultiRemapMessage(InputStream is) throws IOException {
        final byte[] requestBytes = readPayload(is);
        return GroupMultiRemapMessage.parseFrom(requestBytes);
    }

    public static GroupRemapMessage readGroupRemapMessage(InputStream is) throws IOException {
        final byte[] request = readPayload(is);
        return GroupRemapMessage.parseFrom(request);
    }

    public static ImhotepResponse readResponse(InputStream is) throws IOException {
        final byte[] responseBytes = readPayload(is);
        return ImhotepResponse.parseFrom(responseBytes);
    }

    public static ImhotepFrontendRequest readFrontendRequest(InputStream is) throws IOException {
        final byte[] requestBytes = readPayload(is);
        return ImhotepFrontendRequest.parseFrom(requestBytes);
    }

    public static ImhotepFrontendResponse readFrontendResponse(InputStream is) throws IOException {
        final byte[] responseBytes = readPayload(is);
        return ImhotepFrontendResponse.parseFrom(responseBytes);
    }

    private static byte[] readPayload(InputStream is) throws IOException {
        final byte[] payloadLengthBytes = new byte[4];
        ByteStreams.readFully(is, payloadLengthBytes);
        final int payloadLength = Bytes.bytesToInt(payloadLengthBytes);
        final byte[] ret = new byte[payloadLength];
        int index = 0;
        while (index < payloadLength) {
            final int bytesRead = is.read(ret, index, payloadLength-index);
            if (bytesRead <= 0) break;
            index += bytesRead;
        }
        if (index != payloadLength) throw new IOException("invalid message, incorrect payloadLength: expected " + payloadLength + " bytes, got  " + index + " bytes");
        return ret;
    }
}
