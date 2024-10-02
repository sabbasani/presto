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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class TestH2DatabaseSetup
{
    private TestH2DatabaseSetup()
    {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
    public static void setup() throws Exception
    {
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection("jdbc:h2:mem:testdb", "sa", "");

        Statement stmt = conn.createStatement();

        stmt.execute("CREATE SCHEMA IF NOT EXISTS testdb");

        stmt.execute("CREATE TABLE IF NOT EXISTS testdb.example_table1 (id INT PRIMARY KEY, name VARCHAR(255), birthdate DATE, salary DECIMAL(10, 2), active BOOLEAN)");
        stmt.execute("INSERT INTO testdb.example_table1 (id, name, birthdate, salary, active) VALUES (1, 'John Doe', '1990-05-15', 50000.00, TRUE), (2, 'Jane Smith', '1985-11-20', 60000.00, FALSE)");

        stmt.execute("CREATE TABLE IF NOT EXISTS testdb.example_table2 (id INT PRIMARY KEY, description TEXT, quantity INT, price DOUBLE)");
        stmt.execute("INSERT INTO testdb.example_table2 (id, description, quantity, price) VALUES (1, 'Product A', 10, 19.99), (2, 'Product B', 5, 29.99)");

        stmt.execute("DROP TABLE IF EXISTS testdb.example_table3");
        stmt.execute("CREATE TABLE IF NOT EXISTS testdb.example_table3 (id INT PRIMARY KEY, created_at INT, status VARCHAR(255))");
        stmt.execute("INSERT INTO testdb.example_table3 (id, created_at, status) VALUES (1, 36000000, 'A'), (2, 38000000, 'B')");
    }
}
