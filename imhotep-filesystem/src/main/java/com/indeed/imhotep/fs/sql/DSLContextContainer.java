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
