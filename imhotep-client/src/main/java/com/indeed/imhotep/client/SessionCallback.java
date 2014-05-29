package com.indeed.imhotep.client;

import com.indeed.imhotep.api.ImhotepSession;

/**
 * @author dwahler
 */
public interface SessionCallback {
    void handle(ImhotepSession session);
}
