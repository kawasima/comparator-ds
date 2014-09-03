package net.unit8.teslogger.comparator;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.h2.Driver;
import org.h2.jdbcx.JdbcDataSourceFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
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
                    "REGISTERED_AT timestamp," +
                    ")");
        } finally {
            conn.close();
        }
    }

    @Test
    public void test() throws SQLException {
        TableSnapshot snapshot = new TableSnapshot();
        snapshot.setDataSource(ds);
        Connection conn = DriverManager.getConnection("jdbc:h2:file:./target/comparator");
        snapshot.setSnapshotConnection(conn);
        snapshot.take("emp");
    }
}
