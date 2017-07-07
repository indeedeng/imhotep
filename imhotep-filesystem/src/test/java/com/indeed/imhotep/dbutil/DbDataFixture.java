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
