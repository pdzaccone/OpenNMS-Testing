package org.opennms.timeseries.impl.memory;

import com.mchange.v2.c3p0.DataSources;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableMetric;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableSample;
import org.opennms.integration.api.v1.timeseries.immutables.ImmutableTag;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class DbManager {

    private static class DbConnectionPoolingManager {

        private static boolean isConnectionPoolInitialized = false;
        private static DataSource dataSource = null;
        private static String databaseName = "";

        private static void initializeConnectionPool() {
            if (isConnectionPoolInitialized) {
                return;
            }
            try {
                DataSource unpooled = DataSources.unpooledDataSource("jdbc:postgresql://localhost:5432/" + databaseName,
                        "postgres",
                        "");
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

        public static void useSpecificConnection(String databaseName) {
            DbConnectionPoolingManager.databaseName = databaseName;
            initializeConnectionPool();
        }
    }

    private boolean isDbInitialized = false;

    private static final String SEPARATOR_TAG = ",";
    private static final String SEPARATOR_TAGS = ";";

    private static final String TABLENAME_METRICS = "METRICS";
    private static final String TABLENAME_SAMPLES = "SAMPLES";
    private static final String TABLENAME_SERIES = "SERIES";

    private static final String DATABASE_NAME = "inmemoryStorage";

    public Connection getConnection() throws SQLException {
        return DbConnectionPoolingManager.getConnection();
    }

    public void releaseConnection(Connection conn) throws SQLException {
        DbConnectionPoolingManager.releaseConnection(conn);
    }

    public List<Sample> findSamplesForMetric(Connection conn, Metric metric, int idMetric) {
        Objects.requireNonNull(conn);
        List<Sample> results = new ArrayList<>();

        try {
            PreparedStatement statement = conn.prepareStatement(getSampleSearchByMetricQuery());
            statement.setInt(1, idMetric);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                int idSample = resultSet.getInt(1);
                results.add(findSample(conn, idSample, metric));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            results.clear();
        }
        return results;
    }

    public Sample findSample(Connection conn, int idSample, Metric metric) {
        Objects.requireNonNull(conn);
        Sample result = null;

        try {
            PreparedStatement statementSample = conn.prepareStatement(getSampleSearchQuery());
            statementSample.setInt(1, idSample);
            ResultSet resultSet = statementSample.executeQuery();
            if (resultSet.next()) {
                ImmutableSample.ImmutableSampleBuilder builder = ImmutableSample.builder();
                builder.metric(metric);
                builder.time(resultSet.getTimestamp(2).toInstant());
                builder.value(resultSet.getDouble(3));
                result = builder.build();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public int addNewTimeSerie(Connection conn, int idMetric, int idSample) {
        Objects.requireNonNull(conn);
        int id = -1;

        try {
            PreparedStatement statementTimeSeries = conn.prepareStatement(getTimeSerieInsertQuery(), Statement.RETURN_GENERATED_KEYS);
            statementTimeSeries.setInt(1, idMetric);
            statementTimeSeries.setInt(2, idSample);
            int affectedRows = statementTimeSeries.executeUpdate();
            if (affectedRows > 0) {
                ResultSet rs = statementTimeSeries.getGeneratedKeys();
                if (rs.next()) {
                    id = rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public int addNewSample(Connection conn, Sample sample) {
        Objects.requireNonNull(conn);
        Objects.requireNonNull(sample);
        int idSample = -1;

        try {
            PreparedStatement statementSample = conn.prepareStatement(getSampleInsertQuery(), Statement.RETURN_GENERATED_KEYS);
            statementSample.setTimestamp(1, Timestamp.from(sample.getTime()));
            statementSample.setDouble(2, sample.getValue());
            int affectedRowsSample = statementSample.executeUpdate();
            if (affectedRowsSample > 0) {
                ResultSet rs = statementSample.getGeneratedKeys();
                if (rs.next()) {
                    idSample = rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return idSample;
    }

    public int addNewMetric(Connection conn, Metric metric) {
        Objects.requireNonNull(conn);
        Objects.requireNonNull(metric);
        int idMetric = -1;

        try {
            PreparedStatement statementMetric = conn.prepareStatement(getMetricInsertQuery(), Statement.RETURN_GENERATED_KEYS);
            statementMetric.setString(1, metric.getKey());
            statementMetric.setString(2, convertSetToString(metric.getIntrinsicTags()));
            statementMetric.setString(3, convertSetToString(metric.getMetaTags()));
            statementMetric.setString(4, convertSetToString(metric.getExternalTags()));
            int affectedRowsMetric = statementMetric.executeUpdate();
            if (affectedRowsMetric > 0) {
                ResultSet rs = statementMetric.getGeneratedKeys();
                if (rs.next()) {
                    idMetric = rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return idMetric;
    }

    public int findMetric(Connection conn, Metric metric) {
        Objects.requireNonNull(conn);
        Objects.requireNonNull(metric);
        int result = -1;
        try {
            PreparedStatement statement = conn.prepareStatement(getMetricSearchQuery());
            statement.setString(1, metric.getKey());
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                result = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<Metric> findAllMetrics(Connection conn) {
        Objects.requireNonNull(conn);
        List<Metric> result = new ArrayList<>();

        try {
            PreparedStatement statement = conn.prepareStatement(getMetricSearchQueryNoParams());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                ImmutableMetric.MetricBuilder mBuilder = new ImmutableMetric.MetricBuilder();
                mBuilder.intrinsicTags(convertStringToSet(resultSet.getString(3)));
                mBuilder.metaTags(convertStringToSet(resultSet.getString(4)));
                mBuilder.externalTags(convertStringToSet(resultSet.getString(5)));
                Metric metric = mBuilder.build();
                result.add(metric);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            result.clear();
        }
        return result;
    }

    public void deleteMetric(Connection conn, int idMetric) {
        Objects.requireNonNull(conn);

        try {
            PreparedStatement statement = conn.prepareStatement(getMetricDeleteQuery());
            statement.setInt(1, idMetric);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public List<Integer> deleteSeriesForMetric(Connection conn, int idMetric) {
        Objects.requireNonNull(conn);
        List<Integer> samples = new ArrayList<>();

        try {
            PreparedStatement statement = conn.prepareStatement(getSampleSearchByMetricQuery());
            statement.setInt(1, idMetric);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                samples.add(resultSet.getInt(1));
            }
            statement = conn.prepareStatement(getTimeSerieDeleteQuery());
            statement.setInt(1, idMetric);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            samples.clear();
        }
        return samples;
    }

    public void deleteSamples(Connection conn, List<Integer> sampleIDs) {
        Objects.requireNonNull(conn);
        Objects.requireNonNull(sampleIDs);

        try {
            if (!sampleIDs.isEmpty()) {
                PreparedStatement statement = conn.prepareStatement(getSampleDeleteQuery(sampleIDs));
                for (int i = 0; i < sampleIDs.size(); i++) {
                    statement.setInt(i + 1, sampleIDs.get(i));
                }
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void initialize() {
        Connection connection = null;
        try {
            connection = DbConnectionPoolingManager.getConnection();
//            if (!isDatabaseFound(connection, DATABASE_NAME)) {
//                createDatabase(connection, DATABASE_NAME);
//            }
//            connection.close();
//            DbConnectionPoolingManager.useSpecificConnection(DATABASE_NAME);
//            connection = DbConnectionPoolingManager.getConnection();
            if (!this.isDatabaseTableFound(connection, TABLENAME_METRICS)) {
                createTable(connection, "CREATE TABLE " + TABLENAME_METRICS + " (id SERIAL PRIMARY KEY, key TEXT, " +
                        "tagsIntr TEXT, tagsMeta TEXT, tagsExtr TEXT)");
            }
            if (!this.isDatabaseTableFound(connection, TABLENAME_SAMPLES)) {
                createTable(connection, "CREATE TABLE " + TABLENAME_SAMPLES + " (id SERIAL PRIMARY KEY, time TIMESTAMP," +
                        " value DOUBLE PRECISION)");
            }
            if (!this.isDatabaseTableFound(connection, TABLENAME_SERIES)) {
                createTable(connection, "CREATE TABLE " + TABLENAME_SERIES + " (id SERIAL PRIMARY KEY, idMetric INTEGER," +
                        " idSample INTEGER, CONSTRAINT fk_m FOREIGN KEY (idMetric) REFERENCES " + TABLENAME_METRICS + " (id), " +
                        "CONSTRAINT fk_s FOREIGN KEY (idSample) REFERENCES " + TABLENAME_SAMPLES + " (id))");
            }
            isDbInitialized = true;
        } catch (SQLException e) {
            e.printStackTrace();
            isDbInitialized = false;
        } finally {
            try {
                DbConnectionPoolingManager.releaseConnection(connection);
            } catch (SQLException e) {
                e.printStackTrace();
                isDbInitialized = false;
            }
        }
    }

    public void dropTables() throws SQLException {
        Connection connection = DbConnectionPoolingManager.getConnection();
        Statement statement = connection.createStatement();
        statement.executeUpdate("DROP TABLE " + TABLENAME_SERIES);
        statement.executeUpdate("DROP TABLE " + TABLENAME_SAMPLES);
        statement.executeUpdate("DROP TABLE " + TABLENAME_METRICS);
//        statement.executeUpdate("DROP DATABASE " + DATABASE_NAME);
        DbConnectionPoolingManager.releaseConnection(connection);
    }

    private boolean isDatabaseFound(Connection conn, String databaseName) {
        boolean isFound = false;
        try {
            PreparedStatement ps = conn.prepareStatement("SELECT datname FROM pg_database WHERE datistemplate = false;");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if (databaseName.equalsIgnoreCase(rs.getString(1))) {
                    isFound = true;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            isFound = false;
        }
        return isFound;
    }

    private void createDatabase(Connection connection, String databaseName) throws SQLException {
        Objects.requireNonNull(connection);
        Statement statement = connection.createStatement();
        statement.executeUpdate("CREATE DATABASE " + databaseName);
    }

    private String getMetricSearchQuery() {
        return "SELECT id FROM " + TABLENAME_METRICS + " WHERE key = ?";
    }

    private String getMetricSearchQueryNoParams() {
        return "SELECT * FROM " + TABLENAME_METRICS;
    }

    private String getMetricInsertQuery() {
        return "INSERT INTO " + TABLENAME_METRICS + " (key, tagsIntr, tagsMeta, tagsExtr) VALUES (?, ?, ?, ?)";
    }

    private String getSampleInsertQuery() {
        return "INSERT INTO " + TABLENAME_SAMPLES + " (time, value) VALUES (?, ?)";
    }

    private String getTimeSerieInsertQuery() {
        return "INSERT INTO " + TABLENAME_SERIES + "(idMetric, idSample) VALUES (?, ?)";
    }

    private String getSampleSearchByMetricQuery() {
        return "SELECT idSample FROM " + TABLENAME_SERIES + " WHERE idMetric = ?";
    }

    private String getSampleSearchQuery() {
        return "SELECT * FROM " + TABLENAME_SAMPLES + " WHERE id = ?";
    }

    private String getTimeSerieDeleteQuery() {
        return "DELETE FROM " + TABLENAME_SERIES + " WHERE idMetric = ?";
    }

    private String getMetricDeleteQuery() {
        return "DELETE FROM " + TABLENAME_METRICS + " WHERE id = ?";
    }

    private String getSampleDeleteQuery(List<Integer> ids) {
        StringBuilder sbSamplesQuery = new StringBuilder("DELETE FROM " + TABLENAME_SAMPLES + " WHERE id in ( ");
        for (int i = 0; i < ids.size() - 1; i++) {
            sbSamplesQuery.append("?, ");
        }
        sbSamplesQuery.append("?)");
        return sbSamplesQuery.toString();
    }

    private boolean doTablesExist(Connection connection) {
        return isDatabaseTableFound(connection, TABLENAME_METRICS) && isDatabaseTableFound(connection, TABLENAME_SAMPLES)
                && isDatabaseTableFound(connection, TABLENAME_SERIES);
    }

    private boolean isDatabaseTableFound(Connection conn, String tableName) {
        Objects.requireNonNull(conn);
        DatabaseMetaData metaData = null;
        try {
            metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, null, tableName, null);
            ResultSet tables1 = metaData.getTables(null, null, tableName.toLowerCase(Locale.ROOT), null);
            return tables.next() || tables1.next();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private void createTable(Connection connection, String query) throws SQLException {
        Objects.requireNonNull(connection);
        Statement statement = connection.createStatement();
        statement.executeUpdate(query);
    }

    private Collection<Tag> convertStringToSet(String input) {
        Set<Tag> result = new HashSet<>();
        for (String tagStr : input.split(SEPARATOR_TAGS)) {
            String[] tagParts = tagStr.split(SEPARATOR_TAG);
            if (tagParts.length == 2) {
                result.add(new ImmutableTag(tagParts[0], tagParts[1]));
            }
        }
        return result;
    }

    private String convertSetToString(Set<Tag> tags) {
        StringBuilder sb = new StringBuilder();
        for (Tag tag : tags) {
            sb.append(tag.getKey());
            sb.append(SEPARATOR_TAG);
            sb.append(tag.getValue());
            sb.append(SEPARATOR_TAGS);
        }
        return sb.toString();
    }
}