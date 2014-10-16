package net.unit8.teslogger.comparator;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author kawasima
 */
public class VersioningTest {
    @Test
    public void test() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test")) {
            Versioning versioning = new Versioning(conn);
            System.out.println(versioning.getNextVersion(new String[]{"TEST_TABLE"}));
            System.out.println(versioning.getCurrentVersion("TEST_TABLE"));
            System.out.println(versioning.getNextVersion(new String[]{"TEST_TABLE"}));
        }
    }
}
