/*
 * Copyright (C) 2014 Indeed Inc.
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
