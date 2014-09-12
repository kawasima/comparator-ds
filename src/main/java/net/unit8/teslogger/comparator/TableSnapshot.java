package net.unit8.teslogger.comparator;

import org.apache.commons.lang3.StringUtils;
import org.h2.table.Column;
import org.h2.value.DataType;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author kawasima
 */
public class TableSnapshot {
    private DataSource dataSource;
    private Connection snapshotConnection;
    private Map<String, List<Column>> tableDefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private long BULK_COUNT = 1000L;
    private TableNameNormalizer normalizer;

    private Versioning versioning;

    public TableSnapshot(DataSource dataSource, String url) {
        setDataSource(dataSource);
        org.h2.Driver.load();
        try (Connection conn = dataSource.getConnection()) {
            snapshotConnection = DriverManager.getConnection(url);
            DatabaseMetaData md = conn.getMetaData();
            if (md.storesUpperCaseIdentifiers()) {
                normalizer = new TableNameNormalizer() {
                    @Override public String normalize(String tableName) {
                        return tableName.toUpperCase();
                    }
                };
            } else if (md.storesLowerCaseIdentifiers()) {
                normalizer = new TableNameNormalizer() {
                    @Override public String normalize(String tableName) {
                        return tableName.toLowerCase();
                    }
                };
            } else {
                normalizer = new TableNameNormalizer() {
                    @Override public String normalize(String tableName) {
                        return tableName;
                    }
                };
            }

        } catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }

    }

    public void take(String tableName) {
        tableName = normalizer.normalize(tableName);
        try (Connection conn = dataSource.getConnection()) {
            if (versioning == null) {
                versioning = new Versioning(snapshotConnection);
            }

            createTable(conn, tableName);
            copyData(conn, tableName);
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public List<String> listCandidate() {
        List<String> tables = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", new String[]{"TABLE"})) {
                while(rs.next()) {
                    tables.add(rs.getString("TABLE_NAME"));
                }
            }
            return tables;
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private List<Column> readMetadata(DatabaseMetaData md, String tableName) throws SQLException {
        List<Column> columns = new ArrayList<Column>();
        try (ResultSet rs = md.getColumns(null, null, tableName, "%")) {
            while(rs.next()) {
                Column column = new Column(
                        rs.getString("COLUMN_NAME"),
                        DataType.convertSQLTypeToValueType(rs.getInt("DATA_TYPE")),
                        rs.getInt("COLUMN_SIZE"),
                        rs.getInt("DECIMAL_DIGITS"),
                        -1
                );
                column.setAutoIncrement("YES".equals(rs.getString("IS_AUTOINCREMENT")), 1, 1);
                column.setNullable(rs.getInt("NULLABLE") == 0);
                columns.add(column);
            }
            tableDefs.put(tableName, columns);
        }

        try (ResultSet rs = md.getPrimaryKeys(null, null, tableName)) {
            while(rs.next()) {
                String pkColumn = rs.getString("COLUMN_NAME");
                for(Column column : columns) {
                    if (column.getName().equals(pkColumn)) {
                        column.setPrimaryKey(true);
                        break;
                    }
                }
            }
        }

        return columns;
    }

    private void dropTable(String tableName, long version) throws SQLException {
        try (Statement stmt = snapshotConnection.createStatement()) {
            stmt.executeUpdate("DROP TABLE " + tableName);
        }
    }
    public void createTable(Connection conn, String tableName) throws SQLException {
        List<Column> columns = tableDefs.get(tableName);
        if (columns == null) {
            columns = readMetadata(conn.getMetaData(), tableName);
        }

        try (Statement stmt = snapshotConnection.createStatement()) {
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE ")
                    .append(versioning.getNextVersion(tableName))
                    .append(" (");
            for (Column column : columns) {
                sql.append("\n")
                        .append(column.getCreateSQL())
                        .append(",");
            }
            sql.append(")");
            stmt.executeUpdate(sql.toString());
        }
    }

    public void copyData(Connection conn, String tableName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {

            String sql = "INSERT INTO " + versioning.getCurrentVersion(tableName)
                    + " VALUES(" + StringUtils.repeat("?", ",", rs.getMetaData().getColumnCount()) + ")";

            try (PreparedStatement snapshotStmt = snapshotConnection.prepareStatement(sql)) {
                int batchCount = 1;
                while(rs.next()) {
                    int i = 1;
                    for(Column column : tableDefs.get(tableName)) {
                        snapshotStmt.setObject(i++, rs.getObject(column.getName()));
                    }
                    snapshotStmt.addBatch();

                    if (batchCount >= BULK_COUNT) {
                        snapshotStmt.executeBatch();
                    }
                }
                snapshotStmt.executeBatch();
            }
            snapshotConnection.commit();
        }

    }

    private Row findSameRow(List<Row> rows, Row targetRow) {
        for(Row row : rows) {
            if (row.same(targetRow))
                return row;
        }
        return null;
    }

    public Diff diffFromPrevious(String tableName) {
        tableName = normalizer.normalize(tableName);
        try (Statement stmt = snapshotConnection.createStatement()) {
            String addSql = "SELECT * FROM "  + versioning.getCurrentVersion(tableName) +
                    " MINUS SELECT * FROM " + versioning.getPreviousVersion(tableName);

            List<Column> columns = tableDefs.get(tableName);
            Diff diff = new Diff(columns);

            try (ResultSet rs = stmt.executeQuery(addSql)) {
                while(rs.next()) {
                    Row row = new Row(columns);
                    for (Column column : columns) {
                        row.add(rs.getString(column.getName()));
                    }
                    diff.add(row);
                }
            }

            String delSql = "SELECT * FROM "  + versioning.getPreviousVersion(tableName) +
                    " MINUS SELECT * FROM " + versioning.getCurrentVersion(tableName);
            try (ResultSet rs = stmt.executeQuery(delSql)) {
                while(rs.next()) {
                    Row row = new Row(columns);
                    for (Column column : columns) {
                        row.add(rs.getString(column.getName()));
                    }
                    Row addRow = findSameRow(diff.getAdd(), row);
                    if (addRow != null) {
                        diff.modify(addRow, row);
                    } else {
                        diff.delete(row);
                    }
                }
            }
            return diff;

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
