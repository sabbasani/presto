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
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueries;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestArrowFlightSmoke
        extends AbstractTestQueries
{
    private static final Logger logger = Logger.get(TestArrowFlightSmoke.class);

    private static RootAllocator allocator;
    private static FlightServer server;
    private static Location serverLocation;
    private QueryRunner queryRunner;

    @BeforeClass
    public void setup() throws Exception
    {
        File certChainFile = new File("src/test/resources/server.crt");
        File privateKeyFile = new File("src/test/resources/server.key");

        allocator = new RootAllocator(Long.MAX_VALUE);
        serverLocation = Location.forGrpcTls("127.0.0.1", 9443);
        server = FlightServer.builder(allocator, serverLocation, new TestArrowServer(allocator))
                .useTls(certChainFile, privateKeyFile)
                .build();
        server.start();
        logger.info("Server listening on port " + server.getPort());
        queryRunner = TestArrowQueryRunner.createQueryRunner();
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        return TestArrowQueryRunner.createQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    public void close() throws InterruptedException
    {
        if (queryRunner != null) {
            queryRunner.close();
            server.close();
            allocator.close();
        }
    }

    @Test
    public void testShowSchemas()
    {
        assertEquals(queryRunner.execute("SHOW SCHEMAS FROM arrow").getRowCount(), 3);
    }

    @Test
    public void testSelectQuery()
    {
        String query1 = "SELECT * FROM testdb.example_table1";
        String expected1 = "1, John Doe, 1990-05-15, 50000.00, true";
        String expected2 = "2, Jane Smith, 1985-11-20, 60000.00, false";

        String result1 = queryRunner.execute(query1).toString();
        assertTrue(result1.contains(expected1), "Expected row not found: " + expected1);
        assertTrue(result1.contains(expected2), "Expected row not found: " + expected2);
    }

    @Test
    public void testShowTables()
    {
        // Ensure the catalog and schema names are correct
        String query = "SHOW TABLES FROM testdb";
        String[] expectedTables = {"example_table1", "example_table2", "example_table3"};

        String result = queryRunner.execute(query).toString();
        for (String expected : expectedTables) {
            assertTrue(result.contains(expected), "Expected table not found: " + expected);
        }
    }

    @Test
    public void testSelectQueryWithWhereClause()
    {
        String query1 = "SELECT * FROM testdb.example_table3 WHERE created_at = 36000000";
        String expected1 = "1, 36000000, A";

        String result1 = queryRunner.execute(query1).toString();
        assertTrue(result1.contains(expected1), "Expected row not found: " + expected1);

        String query2 = "SELECT * FROM testdb.example_table2 WHERE price > 20.00";
        String expected2 = "2, Product B, 5, 29.99";

        String result2 = queryRunner.execute(query2).toString();
        assertTrue(result2.contains(expected2), "Expected row not found: " + expected2);
    }

    @Test
    public void describeQuery()
    {
        String query1 = "DESCRIBE testdb.example_table1";
        assertEquals(queryRunner.execute(query1).getRowCount(), 5);

        String query2 = "DESCRIBE testdb.example_table2";
        assertEquals(queryRunner.execute(query2).getRowCount(), 4);

        String query3 = "DESCRIBE testdb.example_table3";
        assertEquals(queryRunner.execute(query3).getRowCount(), 3);
    }

    @Test
    public void testSelectQueryWithjoinClause()
    {
        String query = "SELECT t1.id, t1.name, t1.birthdate, t1.salary, t1.active, t2.description, t2.quantity, t2.price FROM example_table1 t1 JOIN example_table2 t2 ON t1.id = t2.id";
        assertEquals(queryRunner.execute(query).getRowCount(), 2);
    }

//    @Override
//    @Test
//    public void testAggregationOverUnknown()
//    {
//        System.out.println("1");
////        assertQuery("SELECT clerk, min(totalprice), max(totalprice), min(nullvalue), max(nullvalue) " +
////                "FROM (SELECT clerk, totalprice, null AS nullvalue FROM orders) " +
////                "GROUP BY clerk");
//    }
//    @Override
//    @Test
//    public void testAliasedInInlineView()
//    {
//        System.out.println("2");
////        assertQuery("SELECT x, y FROM (SELECT orderkey x, custkey y FROM orders) U");
//    }
//
//    @Override
//    @Test
//    public void testApproxPercentile()
//    {
//        System.out.println("3");
//    }
//
//    @Override
//    @Test
//    public void testApproxPercentileMerged()
//    {
//        System.out.println("4");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetBigint()
//    {
//        System.out.println("5");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetBigintGroupBy()
//    {
//        System.out.println("6");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetBigintWithMaxError()
//    {
//        System.out.println("7");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetDouble()
//    {
//        System.out.println("8");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetDoubleGroupBy()
//    {
//        System.out.println("9");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetDoubleWithMaxError()
//    {
//        System.out.println("10");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetGroupByWithNulls()
//    {
//        System.out.println("11");
//    }
//
//    @Test
//    @Override
//    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
//    {
//        System.out.println("12");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetOnlyNulls()
//    {
//        System.out.println("13");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetVarchar()
//    {
//        System.out.println("14");
//    }
//
//    @Override
//    @Test
//    public void testApproxSetVarcharGroupBy()
//    {
//        System.out.println("15");
//    }
//
//
//    @Override
//    @Test
//    public void testApproxSetVarcharWithMaxError()
//    {
//        System.out.println("16");
//    }
//
//
//    @Override
//    @Test
//    public void testApproxSetWithNulls()
//    {
//        System.out.println("16");
//    }
//
//    @Override
//    @Test
//    public void testArithmeticNegation()
//    {
//        System.out.println("17");
////        assertQuery("SELECT -custkey FROM orders");
//    }
//
//    @Override
//    @Test
//    public void testArrayAgg()
//    {
//        System.out.println("18");
//    }
//
//    @Override
//    @Test
//    public void testArrays()
//    {
//        System.out.println("19");
//    }
//
//    @Override
//    @Test
//    public void testAssignUniqueId()
//    {
//        System.out.println("20");
//    }
//
//
//    @Override
//    @Test
//    public void testAverageAll()
//    {
//        System.out.println("21");
//    }
//
//    @Override
//    @Test
//    public void testCaseInsensitiveAliasedRelation()
//    {
//        System.out.println("22");
//    }
//
//
//    @Override
//    @Test
//    public void testCaseInsensitiveAttribute()
//    {
//        System.out.println("23");
//    }
//
//    @Override
//    @Test
//    public void testCaseNoElse()
//    {
//        System.out.println("24");
//    }
//
//    @Override
//    @Test
//    public void testArrayShuffle()
//    {
//        System.out.println("25");
//    }
//
//    @Override
//    @Test
//    public void testCasePredicateRewrite()
//    {
//        System.out.println("26");
//    }
//
//
//    @Override
//    @Test
//    public void testCast()
//    {
//        System.out.println("27");
//    }
//
//    @Override
//    @Test
//    public void testChainedUnionsWithOrder()
//    {
//        System.out.println("28");
//    }
//
//
//    @Override
//    @Test
//    public void testColumnAliases()
//    {
//        System.out.println("29");
//    }
//
//    @Override
//    @Test
//    public void testComparisonWithLike()
//    {
//        System.out.println("30");
//    }
//
//
//    @Override
//    @Test
//    public void testCorrelatedExistsSubqueries()
//    {
//        System.out.println("31");
//    }
//
//    @Override
//    @Test
//    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere()
//    {
//        System.out.println("32");
//    }
//
//    @Override
//    @Test
//    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols()
//    {
//        System.out.println("33");
//    }
//
//    @Override
//    @Test
//    public void testCorrelatedInPredicateSubqueries()
//    {
//        System.out.println("34");
//    }
//
//    @Override
//    @Test
//    public void testCorrelatedNonAggregationScalarSubqueries()
//    {
//        System.out.println("35");
//    }
//
//    @Override
//    @Test
//    public void testCorrelatedScalarSubqueries()
//    {
//        System.out.println("36");
//    }
//
//
//    @Override
//    @Test
//    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
//    {
//        System.out.println("37");
//    }
//
//    @Override
//    @Test
//    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere()
//    {
//        System.out.println("38");
//    }
//
//    @Override
//    @Test
//    public void testCountAll()
//    {
//        System.out.println("39");
//    }
//
//    @Override
//    @Test
//    public void testCountColumn()
//    {
//        System.out.println("40");
//    }
//
//    @Override
//    @Test
//    public void testDependentWindowFunction()
//    {
//        System.out.println("41");
//    }
//
//    @Override
//    @Test
//    public void testDistinct()
//    {
//        System.out.println("42");
//    }
//
//    @Override
//    @Test
//    public void testDistinctHaving()
//    {
//        System.out.println("43");
//    }
//
//    @Override
//    @Test
//    public void testDistinctLimit()
//    {
//        System.out.println("44");
//    }
//
//    @Override
//    @Test
//    public void testDistinctLimitWithQuickDistinctLimitEnabled()
//    {
//        System.out.println("45");
//    }
//
//
//    @Override
//    @Test
//    public void testDistinctMultipleFields()
//    {
//        System.out.println("46");
//    }
//
//
//    @Override
//    @Test
//    public void testDistinctWithOrderBy()
//    {}
//
//    @Override
//    @Test
//    public void testDuplicateFields()
//    {}
//
//
//    @Override
//    @Test
//    public void testExchangeWithProjectionPushDown()
//    {}
//
//    @Override
//    @Test
//    public void testExecuteUsingWithSubquery()
//    {}
//
//    @Override
//    @Test
//    public void testExistsSubquery()
//    {}
//
//    @Override
//    @Test
//    public void testExistsSubqueryWithGroupBy()
//    {}
//
//    @Override
//    @Test
//    public void testFilterPushdownWithAggregation()
//    {}
//
//    @Override
//    @Test
//    public void testGroupByKeyPredicatePushdown()
//    {}
//
//    @Override
//    @Test
//    public void testGroupByLimit()
//    {}
//
//    @Override
//    @Test
//    public void testGroupByOrderByLimit()
//    {}
//
//    @Override
//    @Test
//    public void testGroupByWithConstants()
//    {}
//
//    @Override
//    @Test
//    public void testGroupingInTableSubquery()
//    {}
//
//    @Override
//    @Test
//    public void testGroupingSets()
//    {}
//
//
//    @Override
//    @Test
//    public void testHaving()
//    {}
//
//    @Override
//    @Test
//    public void testHaving2()
//    {}
//
//    @Override
//    @Test
//    public void testHaving3()
//    {}
//
//    @Override
//    @Test
//    public void testHavingWithoutGroupBy()
//    {}
//
//
//    @Override
//    @Test
//    public void testIfExpression()
//    {}
//
//
//    @Override
//    @Test
//    public void testIn()
//    {}
//
//    @Override
//    @Test
//    public void testInlineView()
//    {}
//
//    @Override
//    @Test
//    public void testInlineViewWithProjections()
//    {}
//
//
//    @Override
//    @Test
//    public void testKeyBasedSampling()
//    {}
//
//
//    @Override
//    @Test
//    public void testLargeInWithHistograms()
//    {}
//
//
//    @Override
//    @Test
//    public void testLeftJoinNullFilterToSemiJoin()
//    {}
//
//    @Override
//    @Test
//    public void testLimitAll()
//    {}
//
//
//    @Override
//    @Test
//    public void testLimitIntMax()
//    {}
//
//    @Override
//    @Test
//    public void testLimit()
//    {}
//
//    @Override
//    @Test
//    public void testLimitWithAggregation()
//    {}
//
//    @Override
//    @Test
//    public void testMaps()
//    {}
//
//    @Override
//    @Test
//    public void testMaxBy()
//    {}
//
//    @Override
//    @Test
//    public void testLimitInInlineView()
//    {}
//
//
//    @Override
//    @Test
//    public void testMaxByN()
//    {}
//
//    @Override
//    @Test
//    public void testMaxMinStringWithNulls()
//    {}
//
//    @Override
//    @Test
//    public void testMergeDuplicateAggregations()
//    {}
//
//    @Override
//    @Test
//    public void testMergeEmptyApproxSet()
//    {}
//
//
//    @Override
//    @Test
//    public void testMergeHyperLogLogOnlyNulls()
//    {}
//
//
//    @Override
//    @Test
//    public void testMinBy()
//    {}
//
//    @Test
//    public void testMinByN()
//    {}
//
//    @Override
//    @Test
//    public void testMinMaxN()
//    {}
//
//    @Override
//    @Test
//    public void testMixedWildcards()
//    {}
//
//    @Override
//    @Test
//    public void testMultiColumnUnionAll()
//    {}
//
//    @Override
//    @Test
//    public void testMultipleWildcards()
//    {}
//
//    @Override
//    @Test
//    public void testNonDeterministicAggregationPredicatePushdown()
//    {}
//
//    @Override
//    @Test
//    public void testNonDeterministic()
//    {}
//
//
//    @Override
//    @Test
//    public void testNonDeterministicTableScanPredicatePushdown()
//    {}
//
//    @Override
//    @Test
//    public void testOrderByCompilation()
//    {}
//
//    @Test
//    public void testLimitPushDown()
//    {}
//
//    @Override
//    @Test
//    public void testNonDeterministicProjection()
//    {}
//
//
//    @Override
//    @Test
//    public void testP4ApproxSetBigint()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetVarchar()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetDouble()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetBigintGroupBy()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetVarcharGroupBy()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetDoubleGroupBy()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetWithNulls()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetOnlyNulls()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
//    {}
//
//    @Override
//    @Test
//    public void testP4ApproxSetGroupByWithNulls()
//    {}
//
//    @Override
//    @Test
//    public void testParitalRowNumberNode()
//    {}
//
//    @Override
//    @Test
//    public void testPreProcessMetastoreCalls()
//    {}
//
//    @Override
//    @Test
//    public void testPredicatePushdown()
//    {}
//
//    @Override
//    @Test
//    public void testPruningCountAggregationOverScalar()
//    {}
//
//    @Override
//    @Test
//    public void testQualifiedWildcard()
//    {}
//
//    @Override
//    @Test
//    public void testQualifiedWildcardFromAlias()
//    {}
//
//    @Override
//    @Test
//    public void testQualifiedWildcardFromInlineView()
//    {}
//
//    @Override
//    @Test
//    public void testQueryWithEmptyInput()
//    {}
//
//    @Override
//    @Test
//    public void testQuotedIdentifiers()
//    {}
//
//    @Override
//    @Test
//    public void testRandomizeNullKeyOuterJoin()
//    {}
//
//    @Override
//    @Test
//    public void testRedundantProjection()
//    {}
//
//
//    @Override
//    @Test
//    public void testReferenceToWithQueryInFromClause()
//    {}
//
//    @Override
//    @Test
//    public void testRemoveCrossJoinWithSingleRowConstantInput()
//    {}
//
//    @Override
//    @Test
//    public void testRemoveRedundantCastToVarcharInJoinClause()
//    {}
//
//    @Override
//    @Test
//    public void testRepeatedAggregations()
//    {}
//
//    @Override
//    @Test
//    public void testRepeatedOutputs()
//    {}
//
//    @Override
//    @Test
//    public void testRepeatedOutputs2()
//    {}
//
//    @Override
//    @Test
//    public void testRollupOverUnion()
//    {}
//
//
//    @Override
//    @Test
//    public void testRowNumberLimit()
//    {}
//
//    @Override
//    @Test
//    public void testRowNumberNoOptimization()
//    {}
//
//    @Override
//    @Test
//    public void testRowNumberPartitionedFilter()
//    {}
//
//    @Override
//    @Test
//    public void testRowNumberPropertyDerivation()
//    {}
//
//    @Override
//    @Test
//    public void testRowSubscript()
//    {}
//
//
//    @Override
//    @Test
//    public void testSameAggregationWithAndWithoutFilter()
//    {}
//
//    @Override
//    @Test
//    public void testSamplingJoinChain()
//    {}
//
//    @Override
//    @Test
//    public void testScalarSubquery()
//    {}
//
//    @Override
//    @Test
//    public void testScalarSubqueryWithGroupBy()
//    {}
//
//    @Override
//    @Test
//    public void testRowNumberUnpartitionedFilter()
//    {}
//
//    @Override
//    @Test
//    public void testSelectCaseInsensitive()
//    {}
//
//    @Override
//    @Test
//    public void testSelectColumnOfNulls()
//    {}
//
//    @Override
//    @Test
//    public void testSelectWithComparison()
//    {}
//
//    @Override
//    @Test
//    public void testSetAgg()
//    {}
//
//    @Override
//    @Test
//    public void testStdDev()
//    {}
//
//    @Override
//    @Test
//    public void testStdDevPop()
//    {}
//
//    @Override
//    @Test
//    public void testSubqueryBody()
//    {}
//
//    @Override
//    @Test
//    public void testSubqueryBodyDoubleOrderby()
//    {}
//
//    @Override
//    @Test
//    public void testSubqueryBodyOrderLimit()
//    {}
//
//    @Override
//    @Test
//    public void testSubqueryBodyProjectedOrderby()
//    {}
}
