package com.indeed.imhotep.client;

import com.indeed.util.core.DataLoadingRunnable;
import com.indeed.util.io.Files;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author jsgroth
 */
public class FileHostsReloader extends DataLoadingRunnable implements HostsReloader {
    private static final Logger log = Logger.getLogger(FileHostsReloader.class);

    private final String hostsFile;

    private volatile List<Host> hosts = Collections.emptyList();

    public FileHostsReloader(String hostsFile) {
        super("FileHostsReloader");

        this.hostsFile = trimProtocol(hostsFile);
        updateHosts();
    }

    private static String trimProtocol(String hostsFile) {
        if (hostsFile.startsWith("file://")) {
            return hostsFile.substring("file://".length());
        }
        return hostsFile;
    }

    @Override
    public List<Host> getHosts() {
        return hosts;
    }

    @Override
    public boolean load() {
        if (!updateHosts()) {
            loadFailed();
            return false;
        }
        return true;
    }

    @Override
    public void shutdown() {
    }

    private boolean updateHosts() {
        try {
            final List<Host> newHosts = new ArrayList<Host>();
            for (String line : Files.readTextFile(hostsFile)) {
                if (line.startsWith("#")) continue;

                final String[] splitLine = line.split(":", 2);
                if (splitLine.length != 2) {
                    log.error("invalid host: "+line);
                    continue;
                }
                newHosts.add(new Host(splitLine[0], Integer.parseInt(splitLine[1])));
            }
            Collections.sort(newHosts);
            log.info("reloaded hosts file, new list of hosts: "+newHosts);
            if (!newHosts.equals(hosts)) {
                hosts = newHosts;
            }
            return true;
        } catch (Exception e) {
            log.error("error loading hosts file", e);
            return false;
        }
    }
}
