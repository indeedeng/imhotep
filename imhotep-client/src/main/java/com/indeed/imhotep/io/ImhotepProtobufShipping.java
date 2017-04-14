/*
 * Copyright (C) 2014 Indeed Inc.
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
 package com.indeed.imhotep.io;

import com.google.common.io.ByteStreams;
import com.google.protobuf.Message;
import com.indeed.imhotep.GroupStatsStreamReader;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.frontend.protobuf.ImhotepFrontendRequest;
import com.indeed.imhotep.frontend.protobuf.ImhotepFrontendResponse;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.DataOutputStream;
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

    public static void sendProtobuf(final Message request, final OutputStream os) throws IOException {
        os.write(Bytes.intToBytes(request.getSerializedSize()));
        request.writeTo(os);
        os.flush();
    }

    public static void writeArray(final long[] array, final OutputStream os) throws IOException {
        final DataOutputStream stream = new DataOutputStream(os);
        for (final long value : array) {
            stream.writeLong(value);
        }
        stream.flush();
    }

    public static GroupStatsIterator readGroupStatsIterator(final InputStream is, final int len) {
        return new GroupStatsStreamReader(is, len);
    }

    public static ImhotepRequest readRequest(final InputStream is) throws IOException {
        return ImhotepRequest.parseFrom(readPayloadStream(is));
    }

    public static GroupMultiRemapMessage readGroupMultiRemapMessage(final InputStream is) throws IOException {
        return GroupMultiRemapMessage.parseFrom(readPayloadStream(is));
    }

    public static GroupRemapMessage readGroupRemapMessage(final InputStream is) throws IOException {
        return GroupRemapMessage.parseFrom(readPayloadStream(is));
    }

    public static ImhotepResponse readResponse(final InputStream is) throws IOException {
        return ImhotepResponse.parseFrom(readPayloadStream(is));
    }

    public static ImhotepFrontendRequest readFrontendRequest(final InputStream is) throws IOException {
        return ImhotepFrontendRequest.parseFrom(readPayloadStream(is));
    }

    public static ImhotepFrontendResponse readFrontendResponse(final InputStream is) throws IOException {
        return ImhotepFrontendResponse.parseFrom(readPayloadStream(is));
    }

    private static InputStream readPayloadStream(final InputStream is) throws IOException {
        final byte[] payloadLengthBytes = new byte[4];
        ByteStreams.readFully(is, payloadLengthBytes);
        final int payloadLength = Bytes.bytesToInt(payloadLengthBytes);

        return ByteStreams.limit(is, payloadLength);
    }
}
