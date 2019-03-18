package com.indeed.imhotep.api;

import com.indeed.imhotep.ImhotepRemoteSession;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Interface for each command that will be send in a Imhotep Batch Request.
 * Each command corresponds to an individual ImhotepRequest.
 */
public interface ImhotepCommand<T> {

    T combine(List<T> subResults);

    void writeToOutputStream(OutputStream os, ImhotepRemoteSession imhotepRemoteSession) throws IOException;

    T readResponse(InputStream is, ImhotepRemoteSession imhotepRemoteSession) throws IOException, ImhotepOutOfMemoryException;

    Class<T> getResultClass();

}
