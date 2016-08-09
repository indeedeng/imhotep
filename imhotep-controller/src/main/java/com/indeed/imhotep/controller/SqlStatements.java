package com.indeed.imhotep.controller;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * Created by darren on 3/3/16.
 */
class SqlStatements {
    private static final String CREATE_DATASET_TBL =
            "CREATE TABLE IF NOT EXISTS controller_datasets "
                    + "(name varchar(255) NOT NULL,"
                    + " id INTEGER NOT NULL AUTO_INCREMENT,"
                    + " PRIMARY KEY (id));";
    private static final String CREATE_DATASET_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS controller_dataset_name_idx ON controller_datasets (name);";
    private static final String CREATE_SHARDS_TBL =
            "CREATE TABLE IF NOT EXISTS controller_shards "
                    + "(dataset_id INTEGER NOT NULL,"
                    + " start_time BIGINT NOT NULL,"
                    + " end_time BIGINT NOT NULL,"
                    + " version BIGINT NOT NULL,"
                    + " format TINYINT NOT NULL,"
                    + " id INTEGER NOT NULL AUTO_INCREMENT,"
                    + " PRIMARY KEY (id));";
    private static final String CREATE_SHARDS_START_TIME_IDX =
            "CREATE INDEX IF NOT EXISTS controller_shards_start_idx ON controller_shards (dataset_id, start_time);";
    private static final String CREATE_SHARDS_END_TIME_IDX =
            "CREATE INDEX IF NOT EXISTS controller_shards_end_idx ON controller_shards (dataset_id, end_time);";
    private static final String CREATE_SHARDS_START_END_TIME_IDX =
            "CREATE INDEX IF NOT EXISTS controller_shards_start_end_idx ON controller_shards (dataset_id, start_time, end_time);";
    private static final String CREATE_FIELD_NAMES_TBL =
            "CREATE TABLE IF NOT EXISTS controller_field_names "
                    + "(name varchar(255) NOT NULL,"
                    + " id INTEGER NOT NULL AUTO_INCREMENT,"
                    + " PRIMARY KEY (id));";
    private static final String CREATE_FIELD_NAMES_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS field_name_idx ON controller_field_names (name);";
    private static final String CREATE_FIELD_IDS_TBL =
            "CREATE TABLE IF NOT EXISTS controller_field_ids "
                    + "(id INTEGER NOT NULL AUTO_INCREMENT,"
                    + " shard_id INTEGER NOT NULL,"
                    + " field_name_id INTEGER NOT NULL,"
                    + " num_docs INTEGER NOT NULL,"
                    + " type TINYINT NOT NULL,"
                    + " PRIMARY KEY (id));";
    private static final String CREATE_FIELD_IDS_IDX =
            "CREATE UNIQUE INDEX IF NOT EXISTS controller_field_ids_idx "
                    + "ON controller_field_ids (shard_id, field_name_id);";


    private static final String INSERT_DATASET_NAME =
            "INSERT INTO controller_datasets (name) VALUES (?);";
    private static final String SELECT_DATASET_ID =
            "SELECT id FROM controller_datasets WHERE name = ?;";


    private static final String INSERT_SHARD =
            "INSERT INTO controller_shards "
                    + "("
                    + " dataset_id,"
                    + " start_time,"
                    + " end_time,"
                    + " version,"
                    + " format"
                    + ")"
                    + " VALUES"
                    + " (?,?,?,?,?);";
    private static final String SELECT_SHARD =
            "SELECT id, version, format FROM controller_shards "
                    + "WHERE dataset_id = ?"
                    + " AND start_time = ?"
                    + " AND end_time = ?;";
    private static final String SELECT_SHARD_RANGE =
            "SELECT id, start_time, end_time, version, format FROM controller_shards "
                    + "WHERE dataset_id = ?"
                    + " AND ((start_time >= ? AND start_time < ?)"
                    + " OR (end_time >= ? AND end_time < ?))"
                    + " ORDER BY start_time ASC;";

    private static final String DELETE_DATASET = "DELETE FROM controller_datasets WHERE id = ?;";
    private static final String DELETE_DATASET_SHARDS = "DELETE FROM controller_shards WHERE dataset_id = ?;";


    private final PreparedStatement INSERT_DATASET_NAME_STATEMENT;
    private final PreparedStatement SELECT_DATASET_ID_STATEMENT; 
    private final PreparedStatement INSERT_SHARD_STATEMENT;
    private final PreparedStatement SELECT_SHARD_STATEMENT;
    private final PreparedStatement SELECT_SHARD_RANGE_STATEMENT;
    private final PreparedStatement DELETE_DATASET_STATEMENT;
    private final PreparedStatement DELETE_DATASET_SHARDS_STATEMENT;

    SqlStatements(Connection connection) throws SQLException {
        INSERT_DATASET_NAME_STATEMENT = connection.prepareStatement(INSERT_DATASET_NAME,
                                                                    Statement.RETURN_GENERATED_KEYS);
        SELECT_DATASET_ID_STATEMENT = connection.prepareStatement(SELECT_DATASET_ID);
        INSERT_SHARD_STATEMENT = connection.prepareStatement(INSERT_SHARD,
                                                             Statement.RETURN_GENERATED_KEYS);
        SELECT_SHARD_STATEMENT = connection.prepareStatement(SELECT_SHARD);
        SELECT_SHARD_RANGE_STATEMENT = connection.prepareStatement(SELECT_SHARD_RANGE);
        DELETE_DATASET_STATEMENT = connection.prepareStatement(DELETE_DATASET);
        DELETE_DATASET_SHARDS_STATEMENT = connection.prepareStatement(DELETE_DATASET_SHARDS);
    }

    public static void execute_CREATE_DATASET_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_DATASET_TBL);
        stmt.close();
    }

    public static void execute_CREATE_DATASET_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_DATASET_IDX);
        stmt.close();
    }

    public static void execute_CREATE_SHARDS_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_SHARDS_TBL);
        stmt.close();
    }

    public static void execute_CREATE_SHARDS_START_TIME_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_SHARDS_START_TIME_IDX);
        stmt.close();
    }

    public static void execute_CREATE_SHARDS_END_TIME_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_SHARDS_END_TIME_IDX);
        stmt.close();
    }

    public static void execute_CREATE_SHARDS_START_END_TIME_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_SHARDS_START_END_TIME_IDX);
        stmt.close();
    }

    public static void execute_CREATE_FIELD_NAMES_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FIELD_NAMES_TBL);
        stmt.close();
    }

    public static void execute_CREATE_FIELD_NAMES_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FIELD_NAMES_IDX);
        stmt.close();
    }

    public static void execute_CREATE_FIELD_IDS_TBL(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FIELD_IDS_TBL);
        stmt.close();
    }

    public static void execute_CREATE_FIELD_IDS_IDX(Connection connection) throws SQLException {
        final Statement stmt = connection.createStatement();

        stmt.executeUpdate(CREATE_FIELD_IDS_IDX);
        stmt.close();
    }

    public int execute_INSERT_DATASET_NAME_STATEMENT(String dataset) throws SQLException {
        final int insertId;

        INSERT_DATASET_NAME_STATEMENT.clearParameters();
        INSERT_DATASET_NAME_STATEMENT.setString(1, dataset);
        INSERT_DATASET_NAME_STATEMENT.executeUpdate();
        final ResultSet rs = INSERT_DATASET_NAME_STATEMENT.getGeneratedKeys();
        if (rs != null && rs.next()) {
            insertId = rs.getInt(1);
        } else {
            insertId = -1;
        }

        if (rs != null) {
            rs.close();
        }
        return insertId;
    }

    public int execute_SELECT_DATASET_ID_STATEMENT(String dataset) throws SQLException {
        final int id;

        SELECT_DATASET_ID_STATEMENT.clearParameters();
        SELECT_DATASET_ID_STATEMENT.setString(1, dataset);
        final ResultSet rs = SELECT_DATASET_ID_STATEMENT.executeQuery();
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

    public ArrayList<ShardInfo> execute_SELECT_SHARD_STATEMENT(int dataset_id,
                                                               long start_time,
                                                               long end_time) throws SQLException {
        final ArrayList<ShardInfo> infos = new ArrayList<>();

        SELECT_SHARD_STATEMENT.clearParameters();
        SELECT_SHARD_STATEMENT.setInt(1, dataset_id);
        SELECT_SHARD_STATEMENT.setLong(2, start_time);
        SELECT_SHARD_STATEMENT.setLong(3, end_time);
        final ResultSet rs = SELECT_SHARD_STATEMENT.executeQuery();
        if (rs != null) {
            while (rs.next()) {
                final ShardInfo si = new ShardInfo(rs.getInt(1),
                                                   dataset_id,
                                                   start_time,
                                                   end_time,
                                                   rs.getLong(2),
                                                   rs.getByte(3));
                infos.add(si);
            }
            rs.close();
        }

        return infos;
    }

    public ArrayList<ShardInfo> execute_SELECT_SHARD_RANGE_STATEMENT(int dataset_id,
                                                                     long start_time,
                                                                     long end_time) throws
            SQLException {
        final ArrayList<ShardInfo> infos = new ArrayList<>();

        SELECT_SHARD_RANGE_STATEMENT.clearParameters();
        SELECT_SHARD_RANGE_STATEMENT.setInt(1, dataset_id);
        SELECT_SHARD_RANGE_STATEMENT.setLong(2, start_time);
        SELECT_SHARD_RANGE_STATEMENT.setLong(3, end_time);
        SELECT_SHARD_RANGE_STATEMENT.setLong(4, start_time);
        SELECT_SHARD_RANGE_STATEMENT.setLong(5, end_time);
        final ResultSet rs = SELECT_SHARD_RANGE_STATEMENT.executeQuery();
        if (rs != null) {
            while (rs.next()) {
                final ShardInfo si = new ShardInfo(rs.getInt(1),
                                                   dataset_id,
                                                   rs.getLong(2),
                                                   rs.getLong(3),
                                                   rs.getLong(4),
                                                   rs.getByte(5));
                infos.add(si);
            }
            rs.close();
        }

        return infos;
    }

    public int execute_INSERT_SHARD_STATEMENT(int dataset_id,
                                              long start_time,
                                              long end_time,
                                              long version,
                                              byte format) throws SQLException {
        final int insertId;

        INSERT_SHARD_STATEMENT.clearParameters();
        INSERT_SHARD_STATEMENT.setInt(1, dataset_id);
        INSERT_SHARD_STATEMENT.setLong(2, start_time);
        INSERT_SHARD_STATEMENT.setLong(3, end_time);
        INSERT_SHARD_STATEMENT.setLong(4, version);
        INSERT_SHARD_STATEMENT.setByte(5, format);

        INSERT_SHARD_STATEMENT.executeUpdate();
        final ResultSet rs = INSERT_SHARD_STATEMENT.getGeneratedKeys();
        if (rs != null && rs.next()) {
            insertId = rs.getInt(1);
        } else {
            insertId = -1;
        }

        if (rs != null) {
            rs.close();
        }
        return insertId;
    }

    public void execute_DELETE_DATASET_STATEMENT(int dataset_id) throws SQLException {
        DELETE_DATASET_STATEMENT.clearParameters();
        DELETE_DATASET_STATEMENT.setInt(1, dataset_id);
        DELETE_DATASET_STATEMENT.executeUpdate();
    }

    public void execute_DELETE_DATASET_SHARDS_STATEMENT(int dataset_id) throws SQLException {
        DELETE_DATASET_SHARDS_STATEMENT.clearParameters();
        DELETE_DATASET_SHARDS_STATEMENT.setInt(1, dataset_id);
        DELETE_DATASET_SHARDS_STATEMENT.executeUpdate();
    }
}
