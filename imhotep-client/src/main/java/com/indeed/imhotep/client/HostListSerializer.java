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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.util.io.Files;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author kenh
 */

public class HostListSerializer {
    private HostListSerializer() {
    }

    public static List<Host> fromFile(final Logger log, final File hostsFile) throws IOException {
        final List<Host> newHosts = new ArrayList<>();
        for (final String line : Files.readTextFileOrDie(hostsFile.toString())) {
            if (line.startsWith("#")) {
                continue;
            }

            final String[] splitLine = line.split(":", 2);
            if (splitLine.length != 2) {
                log.error("invalid host: " + line);
                continue;
            }
            newHosts.add(new Host(splitLine[0], Integer.parseInt(splitLine[1])));
        }
        Collections.sort(newHosts);
        return newHosts;
    }

    public static void toFile(final Logger log, final List<Host> hosts, final File hostsFile) throws IOException {
        final String[] hostsAsStrings = FluentIterable.from(hosts)
                .transform(new Function<Host, String>() {
                    @Override
                    public String apply(final Host host) {
                        return String.format("%s:%d", host.getHostname(), host.getPort());
                    }
                }).toArray(String.class);

        Files.writeToTextFileOrDie(hostsAsStrings, hostsFile.toString());
    }
}
