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

package com.indeed.imhotep.dbutil;

import com.indeed.imhotep.fs.sql.SchemaInitializer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.impl.TableImpl;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

/**
 * @author kenh
 */

public class DbDataFixture extends ExternalResource {
    private final TemporaryFolder tempDir = new TemporaryFolder();
    private final List<? extends TableImpl> tables;
    private HikariDataSource dataSource;

    public DbDataFixture(final List<? extends TableImpl> tables) {
        this.tables = tables;
    }

    public HikariDataSource getDataSource() {
        return dataSource;
    }

    @Override
    protected void before() throws Throwable {
        super.before();

        tempDir.create();

        final File dbFile = tempDir.newFile("db");
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:" + dbFile);

        dataSource = new HikariDataSource(config);
        new SchemaInitializer(dataSource).initialize(tables);
    }

    @Override
    protected void after() {
        dataSource.close();
        tempDir.delete();
        super.after();
    }
}
