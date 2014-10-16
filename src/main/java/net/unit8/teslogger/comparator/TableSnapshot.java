package net.unit8.teslogger.comparator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.h2.table.Column;
import org.h2.value.DataType;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * @author kawasima
 */
public class TableSnapshot {
    private static final Log log = LogFactory.getLog(TableSnapshot.class);
    private String schemaName;
    private DataSource dataSource;
    private Connection snapshotConnection;
    private Map<String, List<Column>> tableDefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private Map<Integer, Integer> maximumScales = new HashMap<Integer, Integer>();
    private Map<Integer, Integer> precisions = new HashMap<Integer, Integer>();

    private long BULK_COUNT = 1000L;
    private TableNameNormalizer normalizer;

    private Versioning versioning;

    public TableSnapshot(DataSource dataSource, String url) {
        setDataSource(dataSource);
        org.h2.Driver.load();
        try (Connection conn = dataSource.getConnection()) {
            snapshotConnection = DriverManager.getConnection(url);
            DatabaseMetaData md = conn.getMetaData();
            if (md.getURL().startsWith("jdbc:oracle:")) {
                schemaName = md.getUserName();
            }
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
            try (ResultSet rs = md.getTypeInfo()) {
                while(rs.next()) {
                    int dataType = rs.getInt("DATA_TYPE");
                    int maximumScale = rs.getInt("MAXIMUM_SCALE");
                    maximumScales.put(dataType, maximumScale);
                    precisions.put(dataType, rs.getInt("PRECISION"));
                }
            }
        } catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }

    }

    public void take(String tableName) {
        take(new String[]{tableName});
    }

    public void take(String[] tableNames) {
        try (Connection conn = dataSource.getConnection()) {
            if (versioning == null) {
                versioning = new Versioning(snapshotConnection);
            }
            for (int i=0; i<tableNames.length; i++) {
                tableNames[i] = normalizer.normalize(tableNames[i]);
            }
            long version = versioning.getNextVersion(tableNames);
            for (String tableName : tableNames) {
                createTable(conn, tableName, version);
                copyData(conn, tableName);
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public List<String> listCandidate() {
        List<String> tables = new ArrayList<>();

        try (Connection conn = dataSource.getConnection()) {
            try (ResultSet rs = conn.getMetaData().getTables(null, schemaName, "%", new String[]{"TABLE"})) {
                while(rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    if (!tableName.contains("$"))
                        tables.add(tableName);
                }
            }
            return tables;
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private List<Column> readMetadata(DatabaseMetaData md, String tableName) throws SQLException {
        List<Column> columns = new ArrayList<Column>();
        try (ResultSet rs = md.getColumns(null, schemaName, tableName, "%")) {
            while(rs.next()) {
                int scale     = rs.getInt("COLUMN_SIZE");
                int precision = rs.getInt("DECIMAL_DIGITS");

                if (scale == 0)
                    scale = -1;
                if (precision < 0)
                    precision = -1;
                Column column = new Column(
                        rs.getString("COLUMN_NAME"),
                        DataType.getTypeByName(rs.getString("TYPE_NAME")).type,
                        scale, precision, -1);

                try {
                    column.setAutoIncrement("YES".equals(rs.getString("IS_AUTOINCREMENT")), 1, 1);
                } catch (SQLException ignore) {
                    // Oracle throws SQLException...
                }
                column.setNullable(rs.getInt("NULLABLE") != 0);
                columns.add(column);
            }
            tableDefs.put(tableName, columns);
        }

        try (ResultSet rs = md.getPrimaryKeys(null, schemaName, tableName)) {
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
    public void createTable(Connection conn, String tableName, long version) throws SQLException {
        List<Column> columns = tableDefs.get(tableName);
        if (columns == null) {
            columns = readMetadata(conn.getMetaData(), tableName);
        }

        try (Statement stmt = snapshotConnection.createStatement()) {
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE ")
                    .append(tableName)
                    .append("_")
                    .append(version)
                    .append(" (");
            for (Column column : columns) {
                sql.append("\n")
                        .append(column.getCreateSQL())
                        .append(",");
            }
            sql.append(")");
            log.debug(sql.toString());
            stmt.executeUpdate(sql.toString());
            conn.commit();
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

    public void dispose() {
        try {
            if (snapshotConnection != null && !snapshotConnection.isClosed()) {
                snapshotConnection.close();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
