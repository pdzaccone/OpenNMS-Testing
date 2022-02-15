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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opennms.integration.api.v1.timeseries.AbstractStorageIntegrationTest;
import org.testcontainers.containers.GenericContainer;

import java.sql.SQLException;

public class InMemoryStorageTest extends AbstractStorageIntegrationTest {

    public static GenericContainer<?> container = null;

    private InMemoryStorage storage = null;

    @BeforeClass
    public static void setUpContainer() {
//        container = new GenericContainer<>("inMemoryStorage/inMemoryStorageDB:latest-pg12")
//                .withExposedPorts(5432)
//                .withEnv("POSTGRES_PASSWORD", "password")
//                .withEnv("TIMESCALEDB_TELEMETRY", "off")
//                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)));
//        container.start();
    }

    @AfterClass
    public static void tearDownContainer() {
//        container.stop();
    }

    @Before
    public void setUp() throws Exception {
        storage = createStorage();
        try {
            storage.initialize();
            super.setUp();
        } catch (SQLException e) {
            storage.dropTables();
        }
    }

    @After
    public void tearDown() throws SQLException {
        storage.dropTables();
    }

    @Override
    protected InMemoryStorage createStorage() {
        return new InMemoryStorage();
    }
}
