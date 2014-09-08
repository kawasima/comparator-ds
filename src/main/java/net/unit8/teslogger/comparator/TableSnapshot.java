package net.unit8.teslogger.comparator;

import org.apache.commons.lang3.StringUtils;
import org.h2.table.Column;
import org.h2.value.DataType;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kawasima
 */
public class TableSnapshot {
    private DataSource dataSource;
    private Connection snapshotConnection;
    private Map<String, List<Column>> tableDefs = new HashMap<String, List<Column>>();
    private long BULK_COUNT = 1000L;

    private Versioning versioning = new Versioning();

    public void take(String tableName) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            createTable(connection, tableName);

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch(SQLException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    private List<Column> readMetadata(DatabaseMetaData md, String tableName) throws SQLException {
        List<Column> columns = new ArrayList<Column>();
        ResultSet rs = md.getColumns(null, null, tableName.toUpperCase(), "%");
        try {
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
        } finally {
            rs.close();
        }

        return columns;
    }

    private void dropTable(String tableName, long version) throws SQLException {
        Statement stmt = snapshotConnection.createStatement();
        try {
            stmt.executeUpdate("DROP TABLE " + tableName);
        } finally {
            stmt.close();
        }
    }
    public void createTable(Connection conn, String tableName) throws SQLException {
        List<Column> columns = tableDefs.get(tableName);
        if (columns == null) {
            columns = readMetadata(conn.getMetaData(), tableName);
        }

        Statement stmt = snapshotConnection.createStatement();

        try {
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE ")
                    .append(versioningTableName(tableName, versioning))
                    .append(" (");
            for (Column column : columns) {
                sql.append("\n")
                        .append(column.getCreateSQL())
                        .append(",");
            }
            sql.append(")");
            stmt.executeUpdate(sql.toString());
        } finally {
            stmt.close();
        }
    }

    public void copyData(Connection conn, String tableName) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            PreparedStatement snapshotStmt = snapshotConnection.prepareStatement(
                    "INSERT INTO " + tableName + "(" + StringUtils.repeat("?", ",", 3) + ")");
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            int batchCount = 0;
            while(rs.next()) {
                int i = 1;
                for(Column column : tableDefs.get(tableName)) {
                    snapshotStmt.setObject(i++, rs.getObject(column.getName()));
                }
                snapshotStmt.addBatch();

                if (batchCount >= BULK_COUNT) {
                    snapshotStmt.executeUpdate();
                }
            }
        } finally {
            stmt.close();
        }

    }

    private String versioningTableName(String tableName, long version) {
        return tableName + "_" + version;
    }
    public void diff(String tableName, long version1, long version2) {
        Statement stmt = null;
        try {
            stmt = snapshotConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 'BEFORE' AS _STATUS, T1.* " +
                "FROM (SELECT * FROM "  + versioningTableName(tableName, version1) +
                " MINUS SELECT * FROM " + versioningTableName(tableName, version2) + ") T1 " +
                " UNION ALL " +
                "SELECT 'AFTER' AS _STATUS, T2.* " +
                "FROM (SELECT * FROM "  + versioningTableName(tableName, version2) +
                " MINUS SELECT * FROM " + versioningTableName(tableName, version1) + ") T2 ");
            List<Column> columns = tableDefs.get(tableName);
            while(rs.next()) {
                for (Column column : columns) {
                    System.out.println(rs.getString(column.getName()));
                }
            }
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch(SQLException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setSnapshotConnection(Connection snapshotConnection) {
        this.snapshotConnection = snapshotConnection;
    }
}
