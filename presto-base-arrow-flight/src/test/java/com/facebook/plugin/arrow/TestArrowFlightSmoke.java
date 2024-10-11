/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestArrowFlightSmoke
        extends AbstractTestQueries
{
    private static final Logger logger = Logger.get(TestArrowFlightSmoke.class);
    private static RootAllocator allocator;
    private static FlightServer server;
    private static Location serverLocation;

    @BeforeClass
    public void setup()
            throws Exception
    {
        File certChainFile = new File("src/test/resources/server.crt");
        File privateKeyFile = new File("src/test/resources/server.key");

        allocator = new RootAllocator(Long.MAX_VALUE);
        serverLocation = Location.forGrpcTls("127.0.0.1", 9443);
        server = FlightServer.builder(allocator, serverLocation, new TestingArrowServer(allocator))
                .useTls(certChainFile, privateKeyFile)
                .build();

        server.start();
        logger.info("Server listening on port " + server.getPort());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TestingArrowQueryRunner.createQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws InterruptedException
    {
        server.close();
        allocator.close();
    }

    @Test
    public void testShowCharColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM member");

        MaterializedResult expectedUnparametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "integer", "", "")
                .row("name", "varchar", "", "")
                .row("sex", "char", "", "")
                .row("state", "char", "", "")
                .build();

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "integer", "", "")
                .row("name", "varchar(50)", "", "")
                .row("sex", "char(1)", "", "")
                .row("state", "char(5)", "", "")
                .build();

        assertTrue(actual.equals(expectedParametrizedVarchar) || actual.equals(expectedUnparametrizedVarchar),
                format("%s matches neither %s nor %s", actual, expectedParametrizedVarchar, expectedUnparametrizedVarchar));
    }

    @Test
    public void testPredicateOnCharColumn()
    {
        MaterializedResult actualRow = computeActual("SELECT * from member WHERE state = 'CD'");
        MaterializedResult expectedRow = resultBuilder(getSession(), INTEGER, createVarcharType(50), createCharType(1), createCharType(5))
                .row(2, "MARY", "F", "CD   ")
                .build();
        assertTrue(actualRow.equals(expectedRow));
    }
}
