package net.unit8.teslogger.comparator;

import net.arnx.jsonic.JSON;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;

/**
 * @author kawasima
 */
public class MetadataOutputer {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: java " + MetadataOutputer.class.getName() + " [database url]");
            System.exit(0);
        }
        Properties props = new Properties();
        props.setProperty("url", args[0]);
        DataSource dataSource = BasicDataSourceFactory.createDataSource(props);

        TableSnapshot snapshot = new TableSnapshot(dataSource, "jdbc:h2:mem:dummy");
        try (Connection conn = dataSource.getConnection()) {
            List<String> tables = snapshot.listCandidate();
            for (String table : tables) {
                snapshot.readMetadata(conn.getMetaData(), table);
            }
        }

        try (FileOutputStream fos = new FileOutputStream("tabledefs.json")) {
            JSON.encode(snapshot.tableDefs, fos);
        }
    }
}
