package net.unit8.teslogger.comparator;

import org.h2.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.UUID;

/**
 * @author kawasima
 */
public class Versioning {
    private Connection conn;

    public Versioning(Connection conn) throws SQLException {
        this.conn = conn;
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(getCreateTableSql());
            conn.commit();
        } finally {
            if (stmt != null)
                stmt.close();
        }

    }

    public String getPreviousVersion(String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT id, table_name, created_at FROM versions "
                        + "WHERE table_name = ? "
                        + "ORDER BY created_at DESC")) {
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next() && rs.next()) {
                return tableName + "_" + rs.getLong("ID");
            } else {
                throw new SQLException("No previous version.");
            }

        } catch(SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public String getCurrentVersion(String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT id, table_name, created_at FROM versions "
                        + "WHERE table_name = ? "
                        + "ORDER BY created_at DESC")) {
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return tableName + "_" + rs.getLong("ID");
            } else {
                throw new SQLException("No current version.");
            }
        } catch(SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public String getNextVersion(String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO versions(table_name) "
                        + "VALUES (?)", Statement.RETURN_GENERATED_KEYS)) {
            stmt.setString(1, tableName);
            stmt.executeUpdate();
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                rs.next();
                return tableName + "_" + rs.getLong(1);
            } finally {
                conn.commit();
            }
        } catch(SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private String getCreateTableSql() {
        InputStream is = Versioning.class.getResourceAsStream("versions.sql");
        if (is == null) throw new IllegalStateException("versions.sql not found.");
        try {
            byte[] buf = IOUtils.readBytesAndClose(is, -1);
            return new String(buf);
        } catch(IOException ex) {
            throw new IllegalStateException("Can't read versions.sql.");
        }
    }
}
