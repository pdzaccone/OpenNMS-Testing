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

import com.codahale.metrics.MetricRegistry;
import org.opennms.integration.api.v1.timeseries.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class InMemoryStorage implements TimeSeriesStorage {

    private final MetricRegistry metrics = new MetricRegistry();
//    private final Meter samplesWritten = metrics.meter("samplesWritten");

    private final DbManager dbManager;

    public InMemoryStorage() {
        this.dbManager = new DbManager();
    }

    @Override
    public void store(final List<Sample> samples) {
        Objects.requireNonNull(samples);
        Connection conn = null;
        try {
            conn = this.dbManager.getConnection();

            for (Sample sample : samples) {
                int idMetric = this.dbManager.findMetric(conn, sample.getMetric());
                if (idMetric == -1) {
                    idMetric = this.dbManager.addNewMetric(conn, sample.getMetric());
                }
                int idSample = this.dbManager.addNewSample(conn, sample);
                if (idMetric != -1 && idSample != -1) {
                    this.dbManager.addNewTimeSerie(conn, idMetric, idSample);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                this.dbManager.releaseConnection(conn);
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
            conn = this.dbManager.getConnection();
            List<Metric> metricsInDB = this.dbManager.findAllMetrics(conn);
            results = metricsInDB.stream().filter(val -> matches(tagMatchers, val)).collect(Collectors.toList());
        } catch (SQLException e) {
            e.printStackTrace();
            results.clear();
        }
        finally {
            try {
                this.dbManager.releaseConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
                results.clear();
            }
        }
        return results;
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
            conn = this.dbManager.getConnection();
            int idMetric = this.dbManager.findMetric(conn, request.getMetric());
            if (idMetric != -1) {
                Metric metric = this.dbManager.findMetric(conn, request.getMetric().getKey());
                List<Sample> samples = this.dbManager.findSamplesForMetric(conn, metric, idMetric);
                results = samples.stream().filter(val -> (val.getTime().isAfter(request.getStart()) && val.getTime().isBefore(request.getEnd()))).collect(Collectors.toList());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            results.clear();
        }
        finally {
            try {
                this.dbManager.releaseConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
                results.clear();
            }
        }
        return results;
    }

    @Override
    public void delete(Metric metric) {
        Objects.requireNonNull(metric);
        Connection conn = null;
        try {
            conn = this.dbManager.getConnection();
            int idMetric = this.dbManager.findMetric(conn, metric);
            if (idMetric != -1) {
                List<Integer> idSamples = this.dbManager.deleteSeriesForMetric(conn, idMetric);
                this.dbManager.deleteSamples(conn, idSamples);
                this.dbManager.deleteMetric(conn, idMetric);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                this.dbManager.releaseConnection(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return this.getClass().getName();
    }

    public void initialize() throws SQLException {
        this.dbManager.initialize();
    }

    public void dropTables() throws SQLException {
        this.dbManager.dropTables();
    }

    public MetricRegistry getMetrics() {
        return metrics;
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
}