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
import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.assertions.Assert;
import com.facebook.presto.tests.AbstractTestQueries;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestArrowFlightSmoke
        extends AbstractTestQueries
{
    private static final Logger logger = Logger.get(TestArrowFlightSmoke.class);
    private static RootAllocator allocator;
    private static FlightServer server;
    private static Location serverLocation;

    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";

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
    public void testShowSchemas()
    {
        assertEquals(getQueryRunner().execute("SHOW SCHEMAS FROM arrow").getRowCount(), 3);
    }

    @Test
    public void testSelectQuery()
    {
        String query1 = "SELECT * FROM testdb.example_table1";
        String expected1 = "1, John Doe, 1990-05-15, 50000.00, true";
        String expected2 = "2, Jane Smith, 1985-11-20, 60000.00, false";

        String result1 = getQueryRunner().execute(query1).toString();
        assertTrue(result1.contains(expected1), "Expected row not found: " + expected1);
        assertTrue(result1.contains(expected2), "Expected row not found: " + expected2);
    }

    @Test
    public void testShowTables()
    {
        // Ensure the catalog and schema names are correct
        String query = "SHOW TABLES FROM testdb";
        String[] expectedTables = {"example_table1", "example_table2", "example_table3"};

        String result = getQueryRunner().execute(query).toString();
        for (String expected : expectedTables) {
            assertTrue(result.contains(expected), "Expected table not found: " + expected);
        }
    }

    @Test
    public void testSelectQueryWithWhereClause()
    {
        String query1 = "SELECT * FROM testdb.example_table3 WHERE created_at = 36000000";
        String expected1 = "1, 36000000, A";

        String result1 = getQueryRunner().execute(query1).toString();
        assertTrue(result1.contains(expected1), "Expected row not found: " + expected1);

        String query2 = "SELECT * FROM testdb.example_table2 WHERE price > 20.00";
        String expected2 = "2, Product B, 5, 29.99";

        String result2 = getQueryRunner().execute(query2).toString();
        assertTrue(result2.contains(expected2), "Expected row not found: " + expected2);
    }

    @Test
    public void describeQuery()
    {
        String query1 = "DESCRIBE testdb.example_table1";
        assertEquals(getQueryRunner().execute(query1).getRowCount(), 5);

        String query2 = "DESCRIBE testdb.example_table2";
        assertEquals(getQueryRunner().execute(query2).getRowCount(), 4);

        String query3 = "DESCRIBE testdb.example_table3";
        assertEquals(getQueryRunner().execute(query3).getRowCount(), 3);
    }

    @Test
    public void testSelectQueryWithjoinClause()
    {
        String query = "SELECT t1.id, t1.name, t1.birthdate, t1.salary, t1.active, t2.description, t2.quantity, t2.price FROM example_table1 t1 JOIN example_table2 t2 ON t1.id = t2.id";
        assertEquals(getQueryRunner().execute(query).getRowCount(), 2);
    }

    @Override
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders");

        MaterializedResult expectedParametrizedVarchar = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "char(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "char(15)", "", "")
                .row("clerk", "char(15)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();

        Assert.assertEquals(actual, expectedParametrizedVarchar);
    }

    @Test
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(114)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }
}
