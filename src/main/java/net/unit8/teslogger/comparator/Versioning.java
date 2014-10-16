package net.unit8.teslogger.comparator;

import org.h2.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;

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
            String sqls = getCreateTableSql();
            for (String sql : sqls.split("\\s;\\s")) {
                stmt.execute(sql);
            }
            conn.commit();
        } finally {
            if (stmt != null)
                stmt.close();
        }

    }

    public String getPreviousVersion(String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT version_id, table_name FROM version_tables "
                        + "WHERE table_name = ? "
                        + "ORDER BY version_id DESC")) {
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next() && rs.next()) {
                return tableName + "_" + rs.getLong("VERSION_ID");
            } else {
                throw new SQLException("No previous version.");
            }

        } catch(SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public String getCurrentVersion(String tableName) {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT version_id, table_name FROM version_tables "
                        + "WHERE table_name = ? "
                        + "ORDER BY version_id DESC")) {
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return tableName + "_" + rs.getLong("VERSION_ID");
            } else {
                throw new SQLException("No current version.");
            }
        } catch(SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public long getNextVersion(String[] tableNames) {
        Long version = null;

        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO versions VALUES() ",
                Statement.RETURN_GENERATED_KEYS)) {
            stmt.executeUpdate();
            try (ResultSet rs = stmt.getGeneratedKeys()) {
                rs.next();
                version = rs.getLong(1);
            }
        } catch(SQLException ex) {
            throw new IllegalStateException(ex);
        }

        try (PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO version_tables VALUES(?,?)")) {
            for (String tableName : tableNames) {
                stmt.setLong(1, version);
                stmt.setString(2, tableName);
                stmt.executeUpdate();
            }
            conn.commit();
            return version;
        }  catch(SQLException ex) {
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
