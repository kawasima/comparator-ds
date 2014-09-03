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

    private long version = 0L;

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
    public void createTable(Connection conn, String tableName) throws SQLException {
        List<Column> columns = tableDefs.get(tableName);
        if (columns == null) {
            columns = readMetadata(conn.getMetaData(), tableName);
        }

        Statement stmt = snapshotConnection.createStatement();
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("CREATE TABLE ")
                    .append(tableName)
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

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Connection getSnapshotConnection() {
        return snapshotConnection;
    }

    public void setSnapshotConnection(Connection snapshotConnection) {
        this.snapshotConnection = snapshotConnection;
    }
}
