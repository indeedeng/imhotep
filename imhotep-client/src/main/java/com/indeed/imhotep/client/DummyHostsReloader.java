package com.indeed.imhotep.client;

import java.util.List;

/**
 * @author jsgroth
 */
public class DummyHostsReloader implements HostsReloader {
    private final List<Host> hosts;

    public DummyHostsReloader(List<Host> hosts) {
        this.hosts = hosts;
    }

    @Override
    public List<Host> getHosts() {
        return hosts;
    }

    @Override
    public void run() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public boolean isLoadedDataSuccessfullyRecently() {
        return true;
    }
}
