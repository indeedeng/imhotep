package com.indeed.imhotep.fs;

import com.indeed.imhotep.archive.FileMetadata;
import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * @author darren
 */
public class SqlStatements {
    private static final String INSERT_SHARD_NAME =
            "INSERT INTO cache_sqar_names (name) VALUES (?);";
    private static final String SELECT_SHARD_ID =
            "SELECT id FROM cache_sqar_names WHERE name = ?;";
    private static final String INSERT_FILE_NAME =
            "INSERT INTO cache_file_names (name) VALUES (?);";
    private static final String SELECT_FILE_NAME_ID =
            "SELECT id FROM cache_file_names WHERE name = ?;";
    private static final String INSERT_FILE_JOIN_VALUE =
            "INSERT INTO cache_file_ids "
                    + "(sqar_id, file_name_id, type) "
                    + "VALUES (?, ?, ?);";
    private static final String SELECT_FILE_ID =
            "SELECT id FROM cache_file_ids WHERE sqar_id = ? AND file_name_id = ?;";
    private static final String INSERT_ARCHIVE_NAME =
            "INSERT INTO cache_archive_names (name) VALUES (?);";
    private static final String SELECT_ARCHIVE_ID =
            "SELECT id FROM cache_archive_names WHERE name = ?;";
    private static final String INSERT_FILE_INFO =
            "INSERT INTO cache_file_info "
                    + "(file_id, archive_id, archive_offset, "
                    + "unpacked_size, packed_size, timestamp, sig_hi, sig_low,"
                    + "compressor_type, is_file) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

    private static final String CREATE_SQAR_TBL =
            "CREATE TABLE IF NOT EXISTS cache_sqar_names "
            + "(name varchar(255), id INTEGER IDENTITY);";
    private static final String CREATE_SQAR_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS cache_sqar_name_idx ON cache_sqar_names (name);";
    private static final String CREATE_FILE_NAMES_TBL =
            "CREATE TABLE IF NOT EXISTS cache_file_names "
            + "(name varchar(255), id INTEGER IDENTITY);";
    private static final String CREATE_FILE_NAMES_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS cache_file_name_idx ON cache_file_names (name);";
    private static final String CREATE_FILE_IDS_TBL =
            "CREATE TABLE IF NOT EXISTS cache_file_ids "
            + "(id INTEGER IDENTITY, sqar_id INTEGER, file_name_id INTEGER, type INTEGER);";
    private static final String CREATE_FILE_IDS_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS cache_file_ids_idx "
                    + "ON cache_file_ids (sqar_id, file_name_id);";
    private static final String CREATE_FILENAME_2_ID_VIEW =
            "CREATE VIEW IF NOT EXISTS cache_file_name_to_id "
                    + "AS "
                    + "SELECT cache_file_ids.id, cache_file_ids.sqar_id, cache_file_names.name, cache_file_ids.type "
                    + "FROM cache_file_names INNER JOIN cache_file_ids "
                    + "WHERE cache_file_ids.file_name_id = cache_file_names.id;";
    private static final String CREATE_ARCHIVE_TBL =
            "CREATE TABLE IF NOT EXISTS cache_archive_names "
            + "(name varchar(255), id INTEGER IDENTITY);";
    private static final String CREATE_ARCHIVE_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS cache_archive_name_idx ON cache_archive_names (name);";
    private static final String CREATE_FILE_INFO_TBL =
            "CREATE TABLE IF NOT EXISTS cache_file_info " +
            "(file_id INTEGER PRIMARY KEY, " +
            "archive_id INTEGER, " +
            "archive_offset BIGINT, " +
            "unpacked_size BIGINT, " +
            "packed_size BIGINT, " +
            "timestamp BIGINT, " +
            "sig_hi BIGINT, " +
            "sig_low BIGINT," +
            "compressor_type VARCHAR(16)," +
            "is_file INTEGER);";

    private static final String SELECT_SQAR_ID =
            "SELECT id FROM cache_sqar_names WHERE name = ?;";

    private static final String SELECT_FILE_INFO =
            "SELECT " +
            "cache_file_info.archive_id, " +
            "cache_file_info.archive_offset, " +
            "cache_file_info.unpacked_size, " +
            "cache_file_info.packed_size, " +
            "cache_file_info.timestamp, " +
            "cache_file_info.sig_hi, " +
            "cache_file_info.sig_low, " +
            "cache_file_info.compressor_type, " +
            "cache_file_info.is_file " +
            "FROM cache_file_name_to_id " +
            "INNER JOIN cache_file_info " +
            "ON cache_file_name_to_id.id = cache_file_info.file_id " +
            "WHERE cache_file_name_to_id.sqar_id = ? AND cache_file_name_to_id.name = ?;";
    private static final String SELECT_ARCHIVE_FILE_NAME =
            "SELECT name FROM cache_archive_names WHERE id = ?;";

    private static final String SELECT_FILE_NAMES_WITH_PREFIX =
            "SELECT substr(name, ?3), type "
                    + "FROM cache_file_name_to_id "
                    + "WHERE sqar_id = ?1 "
                    + "AND instr(name, ?2) = 1 " // begins with prefix
                    + "AND instr(substr(name, ?3), '" + SqarMetaDataManager.DELIMITER + "')= 0;"; // delimiter is not in the rest of the filename

    private final PreparedStatement INSERT_SHARD_NAME_STATEMENT;
    private final PreparedStatement SELECT_SHARD_ID_STATEMENT;
    private final PreparedStatement INSERT_FILE_NAME_STATEMENT;
    private final PreparedStatement SELECT_FILE_NAME_ID_STATEMENT;
    private final PreparedStatement INSERT_FILE_JOIN_VALUE_STATEMENT;
    private final PreparedStatement SELECT_FILE_ID_STATEMENT;
    private final PreparedStatement INSERT_ARCHIVE_NAME_STATEMENT;
    private final PreparedStatement SELECT_ARCHIVE_ID_STATEMENT;
    private final PreparedStatement INSERT_FILE_INFO_STATEMENT;

    private final PreparedStatement SELECT_SQAR_ID_STATEMENT;
    private final PreparedStatement SELECT_FILE_INFO_STATEMENT;
    private final PreparedStatement SELECT_ARCHIVE_FILE_NAME_STATEMENT;
    private final PreparedStatement SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT;

    SqlStatements(final Connection connection) throws SQLException {
        INSERT_SHARD_NAME_STATEMENT = connection.prepareStatement(INSERT_SHARD_NAME,
                                                                  Statement.RETURN_GENERATED_KEYS);
        SELECT_SHARD_ID_STATEMENT = connection.prepareStatement(SELECT_SHARD_ID);
        INSERT_FILE_NAME_STATEMENT = connection.prepareStatement(INSERT_FILE_NAME,
                                                                 Statement.RETURN_GENERATED_KEYS);
        SELECT_FILE_NAME_ID_STATEMENT = connection.prepareStatement(SELECT_FILE_NAME_ID);
        INSERT_FILE_JOIN_VALUE_STATEMENT = connection.prepareStatement(INSERT_FILE_JOIN_VALUE,
                                                                       Statement.RETURN_GENERATED_KEYS);
        SELECT_FILE_ID_STATEMENT = connection.prepareStatement(SELECT_FILE_ID);
        INSERT_ARCHIVE_NAME_STATEMENT = connection.prepareStatement(INSERT_ARCHIVE_NAME,
                                                                    Statement.RETURN_GENERATED_KEYS);
        SELECT_ARCHIVE_ID_STATEMENT = connection.prepareStatement(SELECT_ARCHIVE_ID);
        INSERT_FILE_INFO_STATEMENT = connection.prepareStatement(INSERT_FILE_INFO,
                                                                 Statement.RETURN_GENERATED_KEYS);

        SELECT_SQAR_ID_STATEMENT = connection.prepareStatement(SELECT_SQAR_ID);
        SELECT_FILE_INFO_STATEMENT = connection.prepareStatement(SELECT_FILE_INFO);
        SELECT_ARCHIVE_FILE_NAME_STATEMENT = connection.prepareStatement(SELECT_ARCHIVE_FILE_NAME);
        SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT = connection.prepareStatement(
                SELECT_FILE_NAMES_WITH_PREFIX);
    }

    static void execute_CREATE_SQAR_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_SQAR_TBL);
        stmt.close();
    }

    static void execute_CREATE_SQAR_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();
        stmt.executeUpdate(CREATE_SQAR_IDX);
        stmt.close();
    }

    static void execute_CREATE_FILE_NAMES_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FILE_NAMES_TBL);
        stmt.close();
    }

    static void execute_CREATE_FILE_NAMES_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FILE_NAMES_IDX);
        stmt.close();
    }

    static void execute_CREATE_FILE_IDS_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FILE_IDS_TBL);
        stmt.close();
    }

    static void execute_CREATE_FILE_IDS_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FILE_IDS_IDX);
        stmt.close();
    }

    static void execute_CREATE_FILENAME_2_ID_VIEW(Connection connection) throws
            SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FILENAME_2_ID_VIEW);
        stmt.close();
    }

    static void execute_CREATE_ARCHIVE_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_ARCHIVE_TBL);
        stmt.close();
    }

    static void execute_CREATE_ARCHIVE_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_ARCHIVE_IDX);
        stmt.close();
    }

    static void execute_CREATE_FILE_INFO_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FILE_INFO_TBL);
        stmt.close();
    }

    private static int executInsertNameSql(String name, PreparedStatement statement) throws
            SQLException {
        final int id;

        statement.clearParameters();
        statement.setString(1, name);
        statement.executeUpdate();

        final ResultSet rs = statement.getGeneratedKeys();
        if (rs != null && rs.next()) {
            id = rs.getInt(1);
        } else {
            id = -1;
        }
        if (rs != null) {
            rs.close();
        }
        return id;
    }

    private static int executeGetIdSql(String name, PreparedStatement statement) throws
            SQLException {
        final int id;

        statement.clearParameters();
        statement.setString(1, name);
        final ResultSet rs = statement.executeQuery();
        if (rs != null && rs.next()) {
            id = rs.getInt(1);
        } else {
            id = -1;
        }
        if (rs != null) {
            rs.close();
        }
        return id;
    }

    public int execute_INSERT_SHARD_NAME_STATEMENT(String name) throws SQLException {
        return executInsertNameSql(name, INSERT_SHARD_NAME_STATEMENT);
    }

    public int execute_SELECT_SHARD_ID_STATEMENT(String name) throws SQLException {
        return executeGetIdSql(name, SELECT_SHARD_ID_STATEMENT);
    }

    public int execute_INSERT_FILE_NAME_STATEMENT(String name) throws SQLException {
        return executInsertNameSql(name, INSERT_FILE_NAME_STATEMENT);
    }

    public int execute_SELECT_FILE_NAME_ID_STATEMENT(String filename) throws SQLException {
        return executeGetIdSql(filename, SELECT_FILE_NAME_ID_STATEMENT);
    }

    public int execute_INSERT_FILE_JOIN_VALUE_STATEMENT(int sqarId,
                                                        int filenameId,
                                                        boolean isFile) throws SQLException {
        final int id;

        INSERT_FILE_JOIN_VALUE_STATEMENT.clearParameters();
        INSERT_FILE_JOIN_VALUE_STATEMENT.setInt(1, sqarId);
        INSERT_FILE_JOIN_VALUE_STATEMENT.setInt(2, filenameId);
        INSERT_FILE_JOIN_VALUE_STATEMENT.setBoolean(3, isFile);
        INSERT_FILE_JOIN_VALUE_STATEMENT.executeUpdate();

        final ResultSet rs = INSERT_FILE_JOIN_VALUE_STATEMENT.getGeneratedKeys();
        if (rs != null && rs.next()) {
            id = rs.getInt(1);
        } else {
            id = -1;
        }
        if (rs != null) {
            rs.close();
        }
        return id;
    }

    public int execute_SELECT_FILE_ID_STATEMENT(int sqarId, int filenameId) throws SQLException {
        final int id;

        SELECT_FILE_ID_STATEMENT.clearParameters();
        SELECT_FILE_ID_STATEMENT.setInt(1, sqarId);
        SELECT_FILE_ID_STATEMENT.setInt(2, filenameId);
        final ResultSet rs = SELECT_FILE_ID_STATEMENT.executeQuery();
        if (rs != null && rs.next()) {
            id = rs.getInt(1);
        } else {
            id = -1;
        }
        if (rs != null) {
            rs.close();
        }
        return id;
    }

    public int execute_INSERT_ARCHIVE_NAME_STATEMENT(String name) throws SQLException {
        return executInsertNameSql(name, INSERT_ARCHIVE_NAME_STATEMENT);
    }

    public int execute_SELECT_ARCHIVE_ID_STATEMENT(String name) throws SQLException {
        return executeGetIdSql(name, SELECT_ARCHIVE_ID_STATEMENT);
    }

    public int execute_INSERT_FILE_INFO_STATEMENT(int fileId,
                                                  int archiveId,
                                                  long archiveOffset,
                                                  long unpackedSize,
                                                  long packedSize,
                                                  long timestamp,
                                                  long sigHi,
                                                  long sigLow,
                                                  String key,
                                                  boolean isFile) throws SQLException {
        final int id;

        INSERT_FILE_INFO_STATEMENT.clearParameters();
        INSERT_FILE_INFO_STATEMENT.setInt(1, fileId);
        INSERT_FILE_INFO_STATEMENT.setInt(2, archiveId);
        INSERT_FILE_INFO_STATEMENT.setLong(3, archiveOffset);
        INSERT_FILE_INFO_STATEMENT.setLong(4, unpackedSize);
        INSERT_FILE_INFO_STATEMENT.setLong(5, packedSize);
        INSERT_FILE_INFO_STATEMENT.setLong(6, timestamp);
        INSERT_FILE_INFO_STATEMENT.setLong(7, sigHi);
        INSERT_FILE_INFO_STATEMENT.setLong(8, sigLow);
        INSERT_FILE_INFO_STATEMENT.setString(9, key);
        INSERT_FILE_INFO_STATEMENT.setBoolean(10, isFile);

        INSERT_FILE_INFO_STATEMENT.executeUpdate();

        final ResultSet rs = INSERT_FILE_INFO_STATEMENT.getGeneratedKeys();
        if (rs != null && rs.next()) {
            id = rs.getInt(1);
        } else {
            id = -1;
        }
        if (rs != null) {
            rs.close();
        }
        return id;
    }

    public int execute_SELECT_SQAR_ID_STATEMENT(String name) throws SQLException {
        return executeGetIdSql(name, SELECT_SQAR_ID_STATEMENT);
    }

    public FileMetadata execute_SELECT_FILE_INFO_STATEMENT(int sqarId, String filename) throws
            IOException, SQLException {
        SELECT_FILE_INFO_STATEMENT.clearParameters();
        SELECT_FILE_INFO_STATEMENT.setInt(1, sqarId);
        SELECT_FILE_INFO_STATEMENT.setString(2, filename);
        final ResultSet rs = SELECT_FILE_INFO_STATEMENT.executeQuery();

        if (rs != null && rs.next()) {
            final int archiveId = rs.getInt(1);
            final long archiveOffset = rs.getLong(2);
            final long unpackedSize = rs.getLong(3);
            final long packedSize = rs.getLong(4);
            final long timestamp = rs.getLong(5);
            final long sigHi = rs.getLong(6);
            final long sigLow = rs.getLong(7);
            final SquallArchiveCompressor compType = SquallArchiveCompressor.fromKey(
                    rs.getString(8));
            final boolean isFile = rs.getInt(9) != 0;

            if (isFile) {
                final String archiveFileName;
                archiveFileName = execute_SELECT_ARCHIVE_FILE_NAME_STATEMENT(archiveId);
                if (archiveFileName == null) {
                    throw new IOException("Archive name missing. id: " + archiveId);
                }

                return new FileMetadata(filename,
                                        unpackedSize,
                                        packedSize,
                                        timestamp,
                                        sigHi,
                                        sigLow,
                                        archiveOffset,
                                        compType,
                                        archiveFileName,
                                        isFile);
            } else {
                /* path is a directory */
                return new FileMetadata(filename, isFile);
            }
        } else {
                /* no file found */
            return null;
        }
    }

    public String execute_SELECT_ARCHIVE_FILE_NAME_STATEMENT(int id) throws SQLException {
        final String name;

        SELECT_ARCHIVE_FILE_NAME_STATEMENT.clearParameters();
        SELECT_ARCHIVE_FILE_NAME_STATEMENT.setInt(1, id);
        final ResultSet rs = SELECT_ARCHIVE_FILE_NAME_STATEMENT.executeQuery();
        if (rs != null && rs.next()) {
            name = rs.getString(1);
        } else {
            name = null;
        }
        if (rs != null) {
            rs.close();
        }
        return name;
    }

    public ArrayList<FileOrDir> execute_SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT(int sqarId,
                                                                                String prefix) throws
            SQLException {
        SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT.clearParameters();
        SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT.setInt(1, sqarId);
        SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT.setString(2, prefix);
        SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT.setInt(3, prefix.length() + 1); // strings are indexed from 1
        final ResultSet rs = SELECT_FILE_NAMES_WITH_PREFIX_STATEMENT.executeQuery();
        return processFilenames(rs);
    }

    public static class FileOrDir {
        public final String name;
        public final boolean isFile;

        public FileOrDir(String path, boolean isFile) {
            name = path;
            this.isFile = isFile;
        }
    }

    private ArrayList<FileOrDir> processFilenames(ResultSet rs) throws SQLException {
        final ArrayList<FileOrDir> results = new ArrayList<>(1024);

        while ((rs != null) && rs.next()) {
            final String path = rs.getString(1);
            final boolean isFile = rs.getBoolean(2);

            results.add(new FileOrDir(path, isFile));
        }

        return results;
    }
}
