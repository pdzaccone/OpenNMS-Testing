package org.opennms.timeseries.impl.memory;

import com.mchange.v2.c3p0.DataSources;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DbConnectionPoolingManager {

    private static boolean isConnectionPoolInitialized = false;
    private static DataSource dataSource = null;

    private static void initializeConnectionPool() {
        if (isConnectionPoolInitialized) {
            return;
        }
        try {
            DataSource unpooled = DataSources.unpooledDataSource("jdbc:postgresql://localhost/testdb",
                    "swaldman",
                    "test-password");
            dataSource = DataSources.pooledDataSource(unpooled);
            isConnectionPoolInitialized = true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() throws SQLException {
        if (!isConnectionPoolInitialized) {
            initializeConnectionPool();
        }
        return dataSource.getConnection();
    }

    public static void releaseConnection(Connection connection) throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
}
