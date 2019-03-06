package com.indeed.imhotep.api;

import com.indeed.imhotep.ImhotepRemoteSession;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface ImhotepCommand<T> {

    T combine(T... subResults);

    void writeToOutputStream(OutputStream os, ImhotepRemoteSession imhotepRemoteSession) throws IOException;

    T readResponse(InputStream is, ImhotepRemoteSession imhotepRemoteSession) throws IOException, ImhotepOutOfMemoryException;

    T[] getExecutionBuffer(int length);

}
