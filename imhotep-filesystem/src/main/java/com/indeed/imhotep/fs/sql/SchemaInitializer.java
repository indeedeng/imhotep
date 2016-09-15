package com.indeed.imhotep.fs.sql;

import org.apache.commons.io.IOUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author kenh
 */

public class SchemaInitializer {
    private final DataSource dataSource;

    public SchemaInitializer(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private void executeSql(final Path contentPath) throws SQLException, IOException {
        try (Connection connection = dataSource.getConnection();
             final Statement statement = connection.createStatement();
             final InputStream inputStream = Files.newInputStream(contentPath);
        ) {
            final String contents = IOUtils.toString(inputStream);
            statement.execute(contents);
        }
    }

    public void initialize() throws IOException, SQLException, URISyntaxException {
        final Path schemaDir = Paths.get(getClass().getClassLoader().getResource("schema").toURI());
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(schemaDir, new DirectoryStream.Filter<Path>() {
            @Override
            public boolean accept(final Path entry) throws IOException {
                return !Files.isDirectory(entry) && entry.getFileName().toString().endsWith(".sql");
            }
        })) {
            for (final Path entry : directoryStream) {
                executeSql(entry);
            }
        }
    }
}
