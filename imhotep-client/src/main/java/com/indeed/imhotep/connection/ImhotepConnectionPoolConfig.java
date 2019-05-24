package com.indeed.imhotep.connection;

import lombok.Getter;
import lombok.Setter;

/**
 * @author xweng
 */
@Setter
@Getter
public class ImhotepConnectionPoolConfig {
    // We hope the client side time out at first, and then the server socket received EOFException and close it self.
    // The socket time out of server side is 60 seconds, so here we set is as 45 seconds
    private static final int SOCKET_READ_TIMEOUT_MILLIS = 45000;

    // keyedObjectPool doesn't handle the timeout during makeObject, we have to specify it in case of connection block
    private static final int SOCKET_CONNECTING_TIMEOUT_MILLIS = 30000;

    private static final int STATS_REPORT_FREQUENCY_SECONDS = 10;

    private static final int MAX_IDLE_SOCKET_PER_HOST = 16;

    private int socketReadTimeoutMills;

    private int socketConnectingTimeoutMills;

    private int statsReportFrequencySeconds;

    private int maxIdleSocketPerHost;

    public ImhotepConnectionPoolConfig() {
        socketReadTimeoutMills = SOCKET_READ_TIMEOUT_MILLIS;
        socketConnectingTimeoutMills = SOCKET_CONNECTING_TIMEOUT_MILLIS;
        statsReportFrequencySeconds = STATS_REPORT_FREQUENCY_SECONDS;
        maxIdleSocketPerHost = MAX_IDLE_SOCKET_PER_HOST;
    }
}
