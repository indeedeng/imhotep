package com.indeed.imhotep.client;

import com.indeed.util.core.io.Terminable;

import java.util.List;

/**
 * @author jsgroth
 *
 * the run method of this class will be called once a minute so there's no need to have a while (true) loop or anything like that
 */
public interface HostsReloader extends Runnable, Terminable {
    List<Host> getHosts();
    boolean isLoadedDataSuccessfullyRecently();
}
