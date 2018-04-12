/*
 * Copyright (C) 2018 Indeed Inc.
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
 package com.indeed.imhotep.client;

import java.util.List;

/**
 * @author jsgroth
 */
public class DummyHostsReloader implements HostsReloader {
    private final List<Host> hosts;

    public DummyHostsReloader(final List<Host> hosts) {
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
