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

import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertNotEquals;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

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
public class TestArrowTableHandle
{
    @Test
    public void testConstructorAndGetters()
    {
        String schema = "test_schema";
        String table = "test_table";

        // Create an instance of ArrowTableHandle
        ArrowTableHandle tableHandle = new ArrowTableHandle(schema, table);

        // Verify that the schema and table are correctly set
        assertEquals(tableHandle.getSchema(), schema, "Schema should match the input value.");
        assertEquals(tableHandle.getTable(), table, "Table should match the input value.");
    }

    @Test
    public void testToString()
    {
        String schema = "test_schema";
        String table = "test_table";

        // Create an instance of ArrowTableHandle
        ArrowTableHandle tableHandle = new ArrowTableHandle(schema, table);

        // Verify the toString() output
        String expectedToString = "test_schema:test_table";
        assertEquals(tableHandle.toString(), expectedToString, "toString() should return the correct string representation.");
    }

    @Test
    public void testEqualityAndHashCode()
    {
        String schema = "schema";
        String table = "table";

        String schema2 = "schema2";
        String table2 = "table2";

        // Create multiple instances
        ArrowTableHandle handle1 = new ArrowTableHandle(schema, table);
        ArrowTableHandle handle2 = new ArrowTableHandle(schema, table);
        ArrowTableHandle handle3 = new ArrowTableHandle(schema2, table2);

        // Verify equality
        assertEquals(handle1, handle2, "Handles with the same schema and table should be equal.");
        assertNotEquals(handle1, handle3, "Handles with different schema or table should not be equal.");

        // Verify hashCode
        assertEquals(handle1.hashCode(), handle2.hashCode(), "Equal handles should have the same hashCode.");
        assertNotEquals(handle1.hashCode(), handle3.hashCode(), "Different handles should have different hashCodes.");
    }

    @Test
    public void testNullValues()
    {
        String schema = null;
        String table = null;

        // Create an instance of ArrowTableHandle with null values
        ArrowTableHandle tableHandle = new ArrowTableHandle(schema, table);

        // Verify that the schema and table are null
        assertNull(tableHandle.getSchema(), "Schema should be null.");
        assertNull(tableHandle.getTable(), "Table should be null.");

        // Verify the toString() output
        String expectedToString = "null:null";
        assertEquals(tableHandle.toString(), expectedToString, "toString() should handle null values correctly.");
    }
}
