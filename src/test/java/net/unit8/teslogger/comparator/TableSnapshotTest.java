package net.unit8.teslogger.comparator;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.h2.Driver;
import org.h2.store.fs.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.util.Properties;

/**
 * @author kawasima
 */
public class TableSnapshotTest {
    private static DataSource ds;
    @BeforeClass
    public static void setUpDatabase() throws Exception {
        Driver.load();
        Properties props = new Properties();
        props.setProperty("url", "jdbc:h2:mem:test;MODE=ORACLE");
        ds = BasicDataSourceFactory.createDataSource(props);
    }

    @Before
    public void setUp() throws Exception {
        Connection conn = ds.getConnection();
        try {
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("CREATE TABLE emp (" +
                    "ID bigint auto_increment," +
                    "NAME varchar(100)," +
                    "AGE integer," +
                    "REGISTERED_AT timestamp default CURRENT_TIMESTAMP not null," +
                    "PRIMARY KEY (id))");
        } finally {
            conn.close();
        }
    }

    @Test
    public void test() throws SQLException, IOException {
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(Paths.get("./target/"), "comparator.*")) {
            for(Path file : dirStream) {
                Files.delete(file);
            }
        }
        TableSnapshot snapshot = new TableSnapshot();
        snapshot.setDataSource(ds);

        try (Connection conn = DriverManager.getConnection("jdbc:h2:file:./target/comparator")) {
            snapshot.setSnapshotConnection(conn);
            snapshot.take("emp");

            try (Connection targetConn = ds.getConnection();
                 PreparedStatement stmt = targetConn.prepareStatement("INSERT INTO emp(name, age) values (?,?)")) {
                stmt.setString(1, "kawasima");
                stmt.setInt(2, 3);
                stmt.executeUpdate();
                conn.commit();
            }
            snapshot.take("emp");
            snapshot.diffFromPrevious("emp");

            try (Connection targetConn = ds.getConnection();
                 PreparedStatement stmt = targetConn.prepareStatement("UPDATE emp SET age = ? WHERE id = ?")) {
                stmt.setInt(1, 17);
                stmt.setLong(2, 1L);
                stmt.executeUpdate();
                conn.commit();
            }

            snapshot.take("emp");
            snapshot.diffFromPrevious("emp");
        }
    }
}
