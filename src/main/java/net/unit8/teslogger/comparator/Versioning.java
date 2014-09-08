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

    public void init(Connection conn) throws SQLException {
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

    public UUID getCurrentVersion(String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT id, table_name, MAX(created_at) FROM versions "
                        + "WHERE table_name = ? "
                        + "GROUP BY table_name")) {
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return UUID.fromString(rs.getString("ID"));
        } catch(SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public UUID getNextVersion(String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT versions(id, table_name) "
                        + "VALUES (RANDOM_UUID(), ?)")) {
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return UUID.fromString(rs.getString("ID"));
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
