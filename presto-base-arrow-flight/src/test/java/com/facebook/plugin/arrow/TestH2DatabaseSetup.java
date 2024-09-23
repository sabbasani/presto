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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Joiner;
import io.airlift.tpch.TpchTable;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.joda.time.DateTimeZone;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.tpch.TpchTable.PART;
import static io.airlift.tpch.TpchTable.PART_SUPPLIER;
import static io.airlift.tpch.TpchTable.REGION;
import static io.airlift.tpch.TpchTable.SUPPLIER;
import static java.lang.String.format;
import static java.util.Collections.nCopies;

public class TestH2DatabaseSetup
{
    private TestH2DatabaseSetup()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static void setup() throws Exception
    {
        Class.forName("org.h2.Driver");

        String dbUrl = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";

        Connection conn = DriverManager.getConnection(dbUrl, "sa", "");

        Jdbi jdbi = Jdbi.create(dbUrl, "sa", "");
        Handle handle = jdbi.open(); // Get a handle for the database connection

        System.out.println("0.0");

        TpchMetadata tpchMetadata = new TpchMetadata("");

        Statement stmt = conn.createStatement();

        System.out.println("0");

        // Create schema
        stmt.execute("CREATE SCHEMA IF NOT EXISTS testdb");

        // Existing example tables setup
        stmt.execute("CREATE TABLE IF NOT EXISTS testdb.example_table1 (id INT PRIMARY KEY, name VARCHAR(255), birthdate DATE, salary DECIMAL(10, 2), active BOOLEAN)");
        stmt.execute("INSERT INTO testdb.example_table1 (id, name, birthdate, salary, active) VALUES (1, 'John Doe', '1990-05-15', 50000.00, TRUE), (2, 'Jane Smith', '1985-11-20', 60000.00, FALSE)");

        stmt.execute("CREATE TABLE IF NOT EXISTS testdb.example_table2 (id INT PRIMARY KEY, description TEXT, quantity INT, price DOUBLE)");
        stmt.execute("INSERT INTO testdb.example_table2 (id, description, quantity, price) VALUES (1, 'Product A', 10, 19.99), (2, 'Product B', 5, 29.99)");

        stmt.execute("DROP TABLE IF EXISTS testdb.example_table3");
        stmt.execute("CREATE TABLE IF NOT EXISTS testdb.example_table3 (id INT PRIMARY KEY, created_at INT, status VARCHAR(255))");
        stmt.execute("INSERT INTO testdb.example_table3 (id, created_at, status) VALUES (1, 36000000, 'A'), (2, 38000000, 'B')");

        System.out.println("1");

        stmt.execute("CREATE TABLE testdb.orders (\n" +
                "  orderkey BIGINT PRIMARY KEY,\n" +
                "  custkey BIGINT NOT NULL,\n" +
                "  orderstatus CHAR(1) NOT NULL,\n" +
                "  totalprice DOUBLE NOT NULL,\n" +
                "  orderdate DATE NOT NULL,\n" +
                "  orderpriority CHAR(15) NOT NULL,\n" +
                "  clerk CHAR(15) NOT NULL,\n" +
                "  shippriority INTEGER NOT NULL,\n" +
                "  comment VARCHAR(79) NOT NULL\n" +
                ")");
        stmt.execute("CREATE INDEX custkey_index ON testdb.orders (custkey)");
        insertRows(tpchMetadata, ORDERS, handle);

        System.out.println("2");

        handle.execute("CREATE TABLE testdb.lineitem (\n" +
                "  orderkey BIGINT,\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  linenumber INTEGER,\n" +
                "  quantity DOUBLE NOT NULL,\n" +
                "  extendedprice DOUBLE NOT NULL,\n" +
                "  discount DOUBLE NOT NULL,\n" +
                "  tax DOUBLE NOT NULL,\n" +
                "  returnflag CHAR(1) NOT NULL,\n" +
                "  linestatus CHAR(1) NOT NULL,\n" +
                "  shipdate DATE NOT NULL,\n" +
                "  commitdate DATE NOT NULL,\n" +
                "  receiptdate DATE NOT NULL,\n" +
                "  shipinstruct VARCHAR(25) NOT NULL,\n" +
                "  shipmode VARCHAR(10) NOT NULL,\n" +
                "  comment VARCHAR(44) NOT NULL,\n" +
                "  PRIMARY KEY (orderkey, linenumber)" +
                ")");
        insertRows(tpchMetadata, LINE_ITEM, handle);

        System.out.println("3");

        handle.execute(" CREATE TABLE testdb.partsupp (\n" +
                "  partkey BIGINT NOT NULL,\n" +
                "  suppkey BIGINT NOT NULL,\n" +
                "  availqty INTEGER NOT NULL,\n" +
                "  supplycost DOUBLE NOT NULL,\n" +
                "  comment VARCHAR(199) NOT NULL,\n" +
                "  PRIMARY KEY(partkey, suppkey)" +
                ")");
        insertRows(tpchMetadata, PART_SUPPLIER, handle);

        System.out.println("4");

        handle.execute("CREATE TABLE testdb.nation (\n" +
                "  nationkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  regionkey BIGINT NOT NULL,\n" +
                "  comment VARCHAR(114) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, NATION, handle);

        System.out.println("5");

        handle.execute("CREATE TABLE testdb.region(\n" +
                "  regionkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(25) NOT NULL,\n" +
                "  comment VARCHAR(115) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, REGION, handle);
        System.out.println("6");
        handle.execute("CREATE TABLE testdb.part(\n" +
                "  partkey BIGINT PRIMARY KEY,\n" +
                "  name VARCHAR(55) NOT NULL,\n" +
                "  mfgr VARCHAR(25) NOT NULL,\n" +
                "  brand VARCHAR(10) NOT NULL,\n" +
                "  type VARCHAR(25) NOT NULL,\n" +
                "  size INTEGER NOT NULL,\n" +
                "  container VARCHAR(10) NOT NULL,\n" +
                "  retailprice DOUBLE NOT NULL,\n" +
                "  comment VARCHAR(23) NOT NULL\n" +
                ")");
        insertRows(tpchMetadata, PART, handle);
        System.out.println("7");
        handle.execute(" CREATE TABLE testdb.customer (     \n" +
                "    custkey BIGINT NOT NULL,         \n" +
                "    name VARCHAR(25) NOT NULL,       \n" +
                "    address VARCHAR(40) NOT NULL,    \n" +
                "    nationkey BIGINT NOT NULL,       \n" +
                "    phone VARCHAR(15) NOT NULL,      \n" +
                "    acctbal DOUBLE NOT NULL,         \n" +
                "    mktsegment VARCHAR(10) NOT NULL, \n" +
                "    comment VARCHAR(117) NOT NULL    \n" +
                " ) ");
        insertRows(tpchMetadata, CUSTOMER, handle);
        System.out.println("8");
        handle.execute(" CREATE TABLE testdb.supplier ( \n" +
                "    suppkey bigint NOT NULL,         \n" +
                "    name varchar(25) NOT NULL,       \n" +
                "    address varchar(40) NOT NULL,    \n" +
                "    nationkey bigint NOT NULL,       \n" +
                "    phone varchar(15) NOT NULL,      \n" +
                "    acctbal double NOT NULL,         \n" +
                "    comment varchar(101) NOT NULL    \n" +
                " ) ");
        insertRows(tpchMetadata, SUPPLIER, handle);
        System.out.println("9");
//        ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) FROM SUPPLIER");

//        if (resultSet.next()) {
//            int count = resultSet.getInt(1); // Get the first column of the result
//            System.out.println("Number of rows in SUPPLIER table: " + count);
//        }
        ResultSet resultSet1 = stmt.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TESTDB'");

        System.out.println("Tables in 'testdb' schema:");
        while (resultSet1.next()) {
            String tableName = resultSet1.getString("TABLE_NAME");
            System.out.println(tableName);
        }

        ResultSet resultSet = stmt.executeQuery("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA");
        System.out.println("Schemas:");
        while (resultSet.next()) {
            String schemaName = resultSet.getString("SCHEMA_NAME");
            System.out.println(schemaName);
        }
    }

    private static void insertRows(TpchMetadata tpchMetadata, TpchTable tpchTable, Handle handle)
    {
        TpchTableHandle tableHandle = tpchMetadata.getTableHandle(null, new SchemaTableName(TINY_SCHEMA_NAME, tpchTable.getTableName()));
        insertRows(tpchMetadata.getTableMetadata(null, tableHandle), handle, createTpchRecordSet(tpchTable, tableHandle.getScaleFactor()));
    }

    private static void insertRows(ConnectorTableMetadata tableMetadata, Handle handle, RecordSet data)
    {
        List<ColumnMetadata> columns = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !columnMetadata.isHidden())
                .collect(toImmutableList());

        String schemaName = "testdb";
        String tableNameWithSchema = schemaName + "." + tableMetadata.getTable().getTableName();
        String vars = Joiner.on(',').join(nCopies(columns.size(), "?"));
        String sql = format("INSERT INTO %s VALUES (%s)", tableNameWithSchema, vars);

        RecordCursor cursor = data.cursor();
        while (true) {
            // insert 1000 rows at a time
            PreparedBatch batch = handle.prepareBatch(sql);
            for (int row = 0; row < 1000; row++) {
                if (!cursor.advanceNextPosition()) {
                    if (batch.size() > 0) {
                        batch.execute();
                    }
                    return;
                }
                for (int column = 0; column < columns.size(); column++) {
                    Type type = columns.get(column).getType();
                    if (BOOLEAN.equals(type)) {
                        batch.bind(column, cursor.getBoolean(column));
                    }
                    else if (BIGINT.equals(type)) {
                        batch.bind(column, cursor.getLong(column));
                    }
                    else if (INTEGER.equals(type)) {
                        batch.bind(column, (int) cursor.getLong(column));
                    }
                    else if (DOUBLE.equals(type)) {
                        batch.bind(column, cursor.getDouble(column));
                    }
                    else if (type instanceof VarcharType) {
                        batch.bind(column, cursor.getSlice(column).toStringUtf8());
                    }
                    else if (DATE.equals(type)) {
                        long millisUtc = TimeUnit.DAYS.toMillis(cursor.getLong(column));
                        // H2 expects dates in to be millis at midnight in the JVM timezone
                        long localMillis = DateTimeZone.UTC.getMillisKeepLocal(DateTimeZone.getDefault(), millisUtc);
                        batch.bind(column, new Date(localMillis));
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported type " + type);
                    }
                }
                batch.add();
            }
            batch.execute();
        }
    }
}
