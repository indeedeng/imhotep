package com.indeed.imhotep.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.protobuf.Message;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.marshal.ImhotepClientMarshaller;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class RequestTools {
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private RequestTools() {
    }

    /**
     * Interface for sending ImhotepRequest to stream.
     * All implementations of the interface must be thread safe.
     */
    @ThreadSafe
    public interface ImhotepRequestSender {

        default void writeToStreamNoFlush(final OutputStream os) throws IOException {
            writeToStreamNoFlush(os, EMPTY_BYTE_ARRAY);
        }
        void writeToStreamNoFlush(final OutputStream os, final byte[] extraBytes) throws IOException;

        ImhotepRequest.RequestType getRequestType();

        @Nullable
        String getSessionId();

        // Wrapper over ImhotepRequest. Request is encoded every time writeToStreamNoFlush is called.
        class Simple implements ImhotepRequestSender {

            private final ImhotepRequest request;

            public Simple(final ImhotepRequest request) {
                this.request = request;
                // request is not thread safe.
                // calling here to fill ImhotepRequest.memoizedSerializedSize
                // otherwise memoizedSerializedSize could be calculated several times by different threads.
                request.getSerializedSize();
            }

            @Override
            public void writeToStreamNoFlush(final OutputStream os, final byte[] extraBytes) throws IOException {
                os.write(Ints.toByteArray(request.getSerializedSize() + extraBytes.length));
                request.writeTo(os);
                os.write(extraBytes);
            }

            @Override
            public ImhotepRequest.RequestType getRequestType() {
                return request.getRequestType();
            }

            @Override
            @Nullable
            public String getSessionId() {
                return request.hasSessionId() ? request.getSessionId() : null;
            }
        }

        // Saving request into memory buffer and sending data from buffer every time writeToStreamNoFlush is called.
        class Cached implements ImhotepRequestSender {

            private final ImhotepRequest.RequestType type;
            @Nullable private final String sessionId;
            private final byte[] cachedRequest;

            public static Cached create(final ImhotepRequest request) {
                final int requestSize = ImhotepProtobufShipping.getFullSizeInStream(request);
                final byte[] bytes = request.toByteArray();
                Preconditions.checkState(((bytes.length + 4) == requestSize), "Unexpected size of cached request");
                return new Cached(bytes, request.hasSessionId() ? request.getSessionId() : null, request.getRequestType());
            }

            public Cached(
                    final byte[] cachedRequest,
                    @Nullable final String sessionId,
                    final ImhotepRequest.RequestType type
            ) {
                this.cachedRequest = cachedRequest;
                this.sessionId = sessionId;
                this.type = type;
            }

            @Override
            public void writeToStreamNoFlush(final OutputStream os, final byte[] extraBytes) throws IOException {
                os.write(Ints.toByteArray(cachedRequest.length + extraBytes.length));
                os.write(cachedRequest);
                os.write(extraBytes);
            }

            @Override
            public ImhotepRequest.RequestType getRequestType() {
                return type;
            }

            @Override
            @Nullable
            public String getSessionId() {
                return sessionId;
            }
        }
    }

    /**
     * Interface to hide implementation details of sending array of GroupMultiRemapRule to stream.
     */
    public interface GroupMultiRemapRuleSender {

        int getRulesCount();

        void writeToStreamNoFlush(final OutputStream os) throws IOException;

        // Wrapper over message array, each message is encoded on every call of writeToStreamNoFlush
        @EqualsAndHashCode
        @ToString
        class Simple implements GroupMultiRemapRuleSender {

            private final Collection<GroupMultiRemapMessage> messages;

            private Simple(final Collection<GroupMultiRemapMessage> messages)
            {
                this.messages = messages;
                // message class in not thread safe.
                // calling here to fill GroupMultiRemapMessage.memoizedSerializedSize in all messages
                // otherwise memoizedSerializedSize could be calculated several times by different threads.
                for (final GroupMultiRemapMessage message : messages) {
                    message.getSerializedSize();
                }
            }

            @Override
            public int getRulesCount() {
                return messages.size();
            }

            @Override
            public void writeToStreamNoFlush(final OutputStream os) throws IOException {
                for (final GroupMultiRemapMessage message : messages) {
                    ImhotepProtobufShipping.sendProtobufNoFlush(message, os);
                }
            }
        }

        // Holder of encoded messages
        @EqualsAndHashCode
        @ToString
        class Cached implements GroupMultiRemapRuleSender {

            private final int rulesCount;
            private final byte[] cachedRules;
            private final int len;

            public Cached(
                    final byte[] cachedRules,
                    final int len,
                    final int rulesCount)
            {
                this.rulesCount = rulesCount;
                this.cachedRules = cachedRules;
                this.len = len;
            }

            @Override
            public int getRulesCount() {
                return rulesCount;
            }

            @Override
            public void writeToStreamNoFlush(final OutputStream os) throws IOException {
                os.write(cachedRules, 0, len);
            }
        }

        static GroupMultiRemapRuleSender createFromRules(
                final Iterator<GroupMultiRemapRule> rawRules,
                final boolean cacheRules) {
            return createFromMessages(Iterators.transform(rawRules, ImhotepClientMarshaller::marshal), cacheRules);
        }

        static GroupMultiRemapRuleSender createFromMessages(
                final Iterator<GroupMultiRemapMessage> rawRules,
                final boolean cacheRules) {
            if (cacheRules) {
                return cacheMessages(rawRules);
            } else {
                final List<GroupMultiRemapMessage> messages = Lists.newArrayList(rawRules);
                return new Simple(messages);
            }
        }

        static GroupMultiRemapRuleSender cacheMessages(final Iterator<GroupMultiRemapMessage> messages) {
            int rulesCount = 0;
            final HackedByteArrayOutputStream cachedRules = new HackedByteArrayOutputStream();
            try {
                while(messages.hasNext()) {
                    ImhotepProtobufShipping.sendProtobufNoFlush(messages.next(), cachedRules);
                    rulesCount++;
                }
            } catch (final IOException ex) {
                throw Throwables.propagate(ex);
            }

            return new GroupMultiRemapRuleSender.Cached(cachedRules.getBuffer(), cachedRules.getCount(), rulesCount);
        }
    }

    // extending ByteArrayOutputStream to have access to its internal buffer.
    // method ByteArrayOutputStream::toByteArray makes a copy of buffer.
    static class HackedByteArrayOutputStream extends ByteArrayOutputStream {
        HackedByteArrayOutputStream() {
        }

        HackedByteArrayOutputStream(final int size) {
            super(size);
        }

        byte[] getBuffer() {
            return buf;
        }

        int getCount() {
            return count;
        }
    }
}
