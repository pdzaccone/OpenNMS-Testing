/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2006-2020 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2020 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.timeseries.impl.memory;

import java.sql.*;
import java.util.*;

import org.opennms.integration.api.v1.timeseries.Aggregation;
import org.opennms.integration.api.v1.timeseries.Metric;
import org.opennms.integration.api.v1.timeseries.Sample;
import org.opennms.integration.api.v1.timeseries.Tag;
import org.opennms.integration.api.v1.timeseries.TagMatcher;
import org.opennms.integration.api.v1.timeseries.TimeSeriesFetchRequest;
import org.opennms.integration.api.v1.timeseries.TimeSeriesStorage;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

/**
 * Simulates a time series storage in memory (Guava cache). The implementation is super simple and not very efficient.
 * For testing and evaluating purposes only, not for production.
 */
public class InMemoryStorage implements TimeSeriesStorage {

    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter samplesWritten = metrics.meter("samplesWritten");

    @Override
    public void store(final List<Sample> samples) {
        Objects.requireNonNull(samples);
        Connection conn = null;
        try {
            conn = DbConnectionPoolingManager.getConnection();
            int count = 0;

            for (Sample sample : samples) {
                String query = "INSERT INTO TimeSeries (Metric, Sample) VALUES (?, ?)";
                PreparedStatement statement = conn.prepareStatement(query);
                statement.setObject(1, sample.getMetric());
                statement.setObject(2, sample);
                statement.addBatch();

                count++;
                if (count % 100 == 0 || count == samples.size()) {
                    statement.executeBatch();
                }
            }
            samplesWritten.mark(samples.size());
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                DbConnectionPoolingManager.releaseConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public List<Metric> findMetrics(Collection<TagMatcher> tagMatchers) {
        Objects.requireNonNull(tagMatchers);
        List<Metric> results = new ArrayList<>();
        if (tagMatchers.isEmpty()) {
            throw new IllegalArgumentException("We expect at least one TagMatcher but none was given.");
        }
        Connection conn = null;
        try {
            conn = DbConnectionPoolingManager.getConnection();
            String query = "SELECT DISTINCT Metric FROM TimeSeries";
            Statement statement = conn.createStatement();
            ResultSet queryRes = statement.executeQuery(query);
            while (queryRes.next()) {
                Metric metric = (Metric) queryRes.getObject("Metric");
                if (this.matches(tagMatchers, metric)) {
                    results.add(metric);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                DbConnectionPoolingManager.releaseConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    /** Each matcher must be matched by at least one tag. */
    private boolean matches(final Collection<TagMatcher> matchers, final Metric metric) {
        final Set<Tag> searchableTags = new HashSet<>(metric.getIntrinsicTags());
        searchableTags.addAll(metric.getMetaTags());

        for (TagMatcher matcher : matchers) {
            if (searchableTags.stream().noneMatch(t -> this.matches(matcher, t))) {
                return false; // this TagMatcher didn't find any matching tag => this Metric is not part of search result;
            }
        }
        return true; // all matched
    }

    private boolean matches(final TagMatcher matcher, final Tag tag) {

        if (!matcher.getKey().equals(tag.getKey())) {
            return false; // not even the key matches => we are done.
        }

        // Tags have always a non null value so we don't have to null check for them.
        if (TagMatcher.Type.EQUALS == matcher.getType()) {
            return tag.getValue().equals(matcher.getValue());
        } else if (TagMatcher.Type.NOT_EQUALS == matcher.getType()) {
            return !tag.getValue().equals(matcher.getValue());
        } else if (TagMatcher.Type.EQUALS_REGEX == matcher.getType()) {
            return tag.getValue().matches(matcher.getValue());
        } else if (TagMatcher.Type.NOT_EQUALS_REGEX == matcher.getType()) {
            return !tag.getValue().matches(matcher.getValue());
        } else {
            throw new IllegalArgumentException("Implement me for " + matcher.getType());
        }
    }

    @Override
    public List<Sample> getTimeseries(TimeSeriesFetchRequest request) {
        Objects.requireNonNull(request);
        List<Sample> results = new ArrayList<>();
        if (request.getAggregation() != Aggregation.NONE) {
            throw new IllegalArgumentException(String.format("Aggregation %s is not supported.", request.getAggregation()));
        }

        Connection conn = null;
        try {
            conn = DbConnectionPoolingManager.getConnection();
            String query = "SELECT Metric, Sample FROM TimeSeries WHERE Metric = ?";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setObject(1, request.getMetric());
            ResultSet queryRes = statement.executeQuery();
            while (queryRes.next()) {
                Sample sample = (Sample) queryRes.getObject("Sample");
                if (sample.getTime().isAfter(request.getStart()) && sample.getTime().isBefore(request.getEnd())) {
                    results.add(sample);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                DbConnectionPoolingManager.releaseConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    @Override
    public void delete(Metric metric) {
        Objects.requireNonNull(metric);
        Connection conn = null;
        try {
            conn = DbConnectionPoolingManager.getConnection();
            String query = "DELETE FROM TimeSeries WHERE Metric = ?";
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setObject(1, metric);
            statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                DbConnectionPoolingManager.releaseConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName();
    }

    public MetricRegistry getMetrics() {
        return metrics;
    }
}
