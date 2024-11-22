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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ArrowPageUtilsTest
{
    private BufferAllocator allocator;

    @BeforeClass
    public void setUp()
    {
        // Initialize the Arrow allocator
        allocator = new RootAllocator(Integer.MAX_VALUE);
        System.out.println("Allocator initialized: " + allocator);
    }

    @Test
    public void testBuildBlockFromBitVector()
    {
        // Create a BitVector and populate it with values
        BitVector bitVector = new BitVector("bitVector", allocator);
        bitVector.allocateNew(3);  // Allocating space for 3 elements

        bitVector.set(0, 1);  // Set value to 1 (true)
        bitVector.set(1, 0);  // Set value to 0 (false)
        bitVector.setNull(2);  // Set null value

        bitVector.setValueCount(3);

        // Build the block from the vector
        Block resultBlock = ArrowPageUtils.buildBlockFromBitVector(bitVector, BooleanType.BOOLEAN);

        // Now verify the result block
        assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
        assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
    }

    @Test
    public void testBuildBlockFromTinyIntVector()
    {
        // Create a TinyIntVector and populate it with values
        TinyIntVector tinyIntVector = new TinyIntVector("tinyIntVector", allocator);
        tinyIntVector.allocateNew(3);  // Allocating space for 3 elements
        tinyIntVector.set(0, 10);
        tinyIntVector.set(1, 20);
        tinyIntVector.setNull(2);  // Set null value

        tinyIntVector.setValueCount(3);

        // Build the block from the vector
        Block resultBlock = ArrowPageUtils.buildBlockFromTinyIntVector(tinyIntVector, TinyintType.TINYINT);

        // Now verify the result block
        assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
        assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
    }

    @Test
    public void testBuildBlockFromSmallIntVector()
    {
        // Create a SmallIntVector and populate it with values
        SmallIntVector smallIntVector = new SmallIntVector("smallIntVector", allocator);
        smallIntVector.allocateNew(3);  // Allocating space for 3 elements
        smallIntVector.set(0, 10);
        smallIntVector.set(1, 20);
        smallIntVector.setNull(2);  // Set null value

        smallIntVector.setValueCount(3);

        // Build the block from the vector
        Block resultBlock = ArrowPageUtils.buildBlockFromSmallIntVector(smallIntVector, SmallintType.SMALLINT);

        // Now verify the result block
        assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
        assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
    }

    @Test
    public void testBuildBlockFromIntVector()
    {
        // Create an IntVector and populate it with values
        IntVector intVector = new IntVector("intVector", allocator);
        intVector.allocateNew(3);  // Allocating space for 3 elements
        intVector.set(0, 10);
        intVector.set(1, 20);
        intVector.set(2, 30);

        intVector.setValueCount(3);

        // Build the block from the vector
        Block resultBlock = ArrowPageUtils.buildBlockFromIntVector(intVector, IntegerType.INTEGER);

        // Now verify the result block
        assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
        assertEquals(10, resultBlock.getInt(0));  // The 1st element should be 10
        assertEquals(20, resultBlock.getInt(1));  // The 2nd element should be 20
        assertEquals(30, resultBlock.getInt(2));  // The 3rd element should be 30
    }

    @Test
    public void testBuildBlockFromBigIntVector()
            throws InstantiationException, IllegalAccessException
    {
        // Create a BigIntVector and populate it with values
        BigIntVector bigIntVector = new BigIntVector("bigIntVector", allocator);
        bigIntVector.allocateNew(3);  // Allocating space for 3 elements

        bigIntVector.set(0, 10L);
        bigIntVector.set(1, 20L);
        bigIntVector.set(2, 30L);

        bigIntVector.setValueCount(3);

        // Build the block from the vector
        Block resultBlock = ArrowPageUtils.buildBlockFromBigIntVector(bigIntVector, BigintType.BIGINT);

        // Now verify the result block
        assertEquals(10L, resultBlock.getInt(0));  // The 1st element should be 10L
        assertEquals(20L, resultBlock.getInt(1));  // The 2nd element should be 20L
        assertEquals(30L, resultBlock.getInt(2));  // The 3rd element should be 30L
    }

    @Test
    public void testBuildBlockFromDecimalVector()
    {
        // Create a DecimalVector and populate it with values
        DecimalVector decimalVector = new DecimalVector("decimalVector", allocator, 10, 2);  // Precision = 10, Scale = 2
        decimalVector.allocateNew(2);  // Allocating space for 2 elements
        decimalVector.set(0, new BigDecimal("123.45"));

        decimalVector.setValueCount(2);

        // Build the block from the vector
        Block resultBlock = ArrowPageUtils.buildBlockFromDecimalVector(decimalVector, DecimalType.createDecimalType(10, 2));

        // Now verify the result block
        assertEquals(2, resultBlock.getPositionCount());  // Should have 2 positions
        assertTrue(resultBlock.isNull(1));  // The 2nd element should be null
    }

    @Test
    public void testBuildBlockFromTimeStampMicroVector()
    {
        // Create a TimeStampMicroVector and populate it with values
        TimeStampMicroVector timestampMicroVector = new TimeStampMicroVector("timestampMicroVector", allocator);
        timestampMicroVector.allocateNew(3);  // Allocating space for 3 elements
        timestampMicroVector.set(0, 1000000L);  // 1 second in microseconds
        timestampMicroVector.set(1, 2000000L);  // 2 seconds in microseconds
        timestampMicroVector.setNull(2);  // Set null value

        timestampMicroVector.setValueCount(3);

        // Build the block from the vector
        Block resultBlock = ArrowPageUtils.buildBlockFromTimeStampMicroVector(timestampMicroVector, TimestampType.TIMESTAMP);

        // Now verify the result block
        assertEquals(3, resultBlock.getPositionCount());  // Should have 3 positions
        assertTrue(resultBlock.isNull(2));  // The 3rd element should be null
        assertEquals(1000L, resultBlock.getLong(0));  // The 1st element should be 1000ms (1 second)
        assertEquals(2000L, resultBlock.getLong(1));  // The 2nd element should be 2000ms (2 seconds)
    }

    @Test
    public void testBuildBlockFromListVector()
    {
        // Create a root allocator for Arrow vectors
        try (BufferAllocator allocator = new RootAllocator();
                ListVector listVector = ListVector.empty("listVector", allocator)) {
            // Allocate the vector and get the writer
            listVector.allocateNew();
            UnionListWriter listWriter = listVector.getWriter();

            int[] data = new int[] {1, 2, 3, 10, 20, 30, 100, 200, 300, 1000, 2000, 3000};
            int tmpIndex = 0;

            for (int i = 0; i < 4; i++) { // 4 lists to be added
                listWriter.startList();
                for (int j = 0; j < 3; j++) { // Each list has 3 integers
                    listWriter.writeInt(data[tmpIndex]);
                    tmpIndex++;
                }
                listWriter.endList();
            }

            // Set the number of lists
            listVector.setValueCount(4);

            // Create Presto ArrayType for Integer
            ArrayType arrayType = new ArrayType(IntegerType.INTEGER);

            // Call the method to test
            Block block = ArrowPageUtils.buildBlockFromListVector(listVector, arrayType);

            // Validate the result
            assertEquals(block.getPositionCount(), 4); // 4 lists in the block
        }
    }
}
