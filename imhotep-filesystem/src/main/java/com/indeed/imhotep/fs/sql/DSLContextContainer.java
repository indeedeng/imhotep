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

package com.indeed.imhotep.fs.sql;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import javax.sql.DataSource;

/**
 * @author kenh
 */

public class DSLContextContainer {
    private final Settings settings = new Settings().withRenderSchema(false);
    private final DataSource dataSource;

    public DSLContextContainer(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DSLContext getDSLContext() {
        return DSL.using(dataSource, SQLDialect.H2, settings);
    }
}
