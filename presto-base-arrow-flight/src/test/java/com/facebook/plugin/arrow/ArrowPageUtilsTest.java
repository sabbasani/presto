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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import io.airlift.slice.Slice;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ArrowPageUtilsTest
{
    private static final int DICTIONARY_LENGTH = 10;
    private static final int VECTOR_LENGTH = 50;
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

    @Test
    public void testProcessDictionaryVector()
    {
        // Create dictionary vector
        VarCharVector dictionaryVector = new VarCharVector("dictionary", allocator);
        dictionaryVector.allocateNew(DICTIONARY_LENGTH);
        for (int i = 0; i < DICTIONARY_LENGTH; i++) {
            dictionaryVector.setSafe(i, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        dictionaryVector.setValueCount(DICTIONARY_LENGTH);

        // Create raw vector
        VarCharVector rawVector = new VarCharVector("raw", allocator);
        rawVector.allocateNew(VECTOR_LENGTH);
        for (int i = 0; i < VECTOR_LENGTH; i++) {
            int value = i % DICTIONARY_LENGTH;
            rawVector.setSafe(i, String.valueOf(value).getBytes(StandardCharsets.UTF_8));
        }
        rawVector.setValueCount(VECTOR_LENGTH);

        // Encode using dictionary
        Dictionary dictionary = new Dictionary(dictionaryVector, new DictionaryEncoding(1L, false, null));
        IntVector encodedVector = (IntVector) DictionaryEncoder.encode(rawVector, dictionary);

        // Process the dictionary vector
        Block result = ArrowPageUtils.buildBlockFromEncodedVector(encodedVector, dictionary);

        // Verify the result
        assertNotNull(result, "The BlockBuilder should not be null.");
        assertEquals(result.getPositionCount(), 50);
    }

    @Test
    public void testHandleVarcharType()
    {
        Type varcharType = VarcharType.createUnboundedVarcharType();
        BlockBuilder builder = varcharType.createBlockBuilder(null, 1);

        String value = "test_string";
        ArrowPageUtils.handleVarcharType(varcharType, builder, value);

        Block block = builder.build();
        Slice result = varcharType.getSlice(block, 0);
        assertEquals(result.toStringUtf8(), value);
    }

    @Test
    public void testHandleSmallintType()
    {
        Type smallintType = SmallintType.SMALLINT;
        BlockBuilder builder = smallintType.createBlockBuilder(null, 1);

        short value = 42;
        ArrowPageUtils.handleSmallintType(smallintType, builder, value);

        Block block = builder.build();
        long result = smallintType.getLong(block, 0);
        assertEquals(result, value);
    }

    @Test
    public void testHandleTinyintType()
    {
        Type tinyintType = TinyintType.TINYINT;
        BlockBuilder builder = tinyintType.createBlockBuilder(null, 1);

        byte value = 7;
        ArrowPageUtils.handleTinyintType(tinyintType, builder, value);

        Block block = builder.build();
        long result = tinyintType.getLong(block, 0);
        assertEquals(result, value);
    }

    @Test
    public void testHandleBigintType()
    {
        Type bigintType = BigintType.BIGINT;
        BlockBuilder builder = bigintType.createBlockBuilder(null, 1);

        long value = 123456789L;
        ArrowPageUtils.handleBigintType(bigintType, builder, value);

        Block block = builder.build();
        long result = bigintType.getLong(block, 0);
        assertEquals(result, value);
    }

    @Test
    public void testHandleIntegerType()
    {
        Type integerType = IntegerType.INTEGER;
        BlockBuilder builder = integerType.createBlockBuilder(null, 1);

        int value = 42;
        ArrowPageUtils.handleIntegerType(integerType, builder, value);

        Block block = builder.build();
        long result = integerType.getLong(block, 0);
        assertEquals(result, value);
    }

    @Test
    public void testHandleDoubleType()
    {
        Type doubleType = DoubleType.DOUBLE;
        BlockBuilder builder = doubleType.createBlockBuilder(null, 1);

        double value = 42.42;
        ArrowPageUtils.handleDoubleType(doubleType, builder, value);

        Block block = builder.build();
        double result = doubleType.getDouble(block, 0);
        assertEquals(result, value, 0.001);
    }

    @Test
    public void testHandleBooleanType()
    {
        Type booleanType = BooleanType.BOOLEAN;
        BlockBuilder builder = booleanType.createBlockBuilder(null, 1);

        boolean value = true;
        ArrowPageUtils.handleBooleanType(booleanType, builder, value);

        Block block = builder.build();
        boolean result = booleanType.getBoolean(block, 0);
        assertEquals(result, value);
    }

    @Test
    public void testHandleArrayType()
    {
        Type elementType = IntegerType.INTEGER;
        ArrayType arrayType = new ArrayType(elementType);
        BlockBuilder builder = arrayType.createBlockBuilder(null, 1);

        List<Integer> values = Arrays.asList(1, 2, 3);
        ArrowPageUtils.handleArrayType(arrayType, builder, values);

        Block block = builder.build();
        Block arrayBlock = arrayType.getObject(block, 0);
        assertEquals(arrayBlock.getPositionCount(), values.size());
        for (int i = 0; i < values.size(); i++) {
            assertEquals(elementType.getLong(arrayBlock, i), values.get(i).longValue());
        }
    }

    @Test
    public void testHandleRowType()
    {
        RowType.Field field1 = new RowType.Field(Optional.of("field1"), IntegerType.INTEGER);
        RowType.Field field2 = new RowType.Field(Optional.of("field2"), VarcharType.createUnboundedVarcharType());
        RowType rowType = RowType.from(Arrays.asList(field1, field2));
        BlockBuilder builder = rowType.createBlockBuilder(null, 1);

        List<Object> rowValues = Arrays.asList(42, "test");
        ArrowPageUtils.handleRowType(rowType, builder, rowValues);

        Block block = builder.build();
        Block rowBlock = rowType.getObject(block, 0);
        assertEquals(IntegerType.INTEGER.getLong(rowBlock, 0), 42);
        assertEquals(VarcharType.createUnboundedVarcharType().getSlice(rowBlock, 1).toStringUtf8(), "test");
    }

    @Test
    public void testHandleDateType()
    {
        Type dateType = DateType.DATE;
        BlockBuilder builder = dateType.createBlockBuilder(null, 1);

        LocalDate value = LocalDate.of(2020, 1, 1);
        ArrowPageUtils.handleDateType(dateType, builder, value);

        Block block = builder.build();
        long result = dateType.getLong(block, 0);
        assertEquals(result, value.toEpochDay());
    }

    @Test
    public void testHandleTimestampType()
    {
        Type timestampType = TimestampType.TIMESTAMP;
        BlockBuilder builder = timestampType.createBlockBuilder(null, 1);

        long value = 1609459200000L; // Jan 1, 2021, 00:00:00 UTC
        ArrowPageUtils.handleTimestampType(timestampType, builder, value);

        Block block = builder.build();
        long result = timestampType.getLong(block, 0);
        assertEquals(result, value);
    }

    @Test
    public void testHandleTimestampTypeWithSqlTimestamp()
    {
        Type timestampType = TimestampType.TIMESTAMP;
        BlockBuilder builder = timestampType.createBlockBuilder(null, 1);

        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf("2021-01-01 00:00:00");
        long expectedMillis = timestamp.getTime();
        ArrowPageUtils.handleTimestampType(timestampType, builder, timestamp);

        Block block = builder.build();
        long result = timestampType.getLong(block, 0);
        assertEquals(result, expectedMillis);
    }

    @Test
    public void testShortDecimalRetrieval()
    {
        DecimalType shortDecimalType = DecimalType.createDecimalType(10, 2); // Precision: 10, Scale: 2
        BlockBuilder builder = shortDecimalType.createBlockBuilder(null, 1);

        BigDecimal decimalValue = new BigDecimal("12345.67");
        ArrowPageUtils.handleDecimalType(shortDecimalType, builder, decimalValue);

        Block block = builder.build();
        long unscaledValue = shortDecimalType.getLong(block, 0); // Unscaled value: 1234567
        BigDecimal result = BigDecimal.valueOf(unscaledValue).movePointLeft(shortDecimalType.getScale());
        assertEquals(result, decimalValue);
    }

    @Test
    public void testLongDecimalRetrieval()
    {
        // Create a DecimalType with precision 38 and scale 10
        DecimalType longDecimalType = DecimalType.createDecimalType(38, 10);
        BlockBuilder builder = longDecimalType.createBlockBuilder(null, 1);
        BigDecimal decimalValue = new BigDecimal("1234567890.1234567890");
        ArrowPageUtils.handleDecimalType(longDecimalType, builder, decimalValue);
        // Build the block after inserting the decimal value
        Block block = builder.build();
        Slice unscaledSlice = longDecimalType.getSlice(block, 0);
        BigInteger unscaledValue = Decimals.decodeUnscaledValue(unscaledSlice);
        BigDecimal result = new BigDecimal(unscaledValue).movePointLeft(longDecimalType.getScale());
        // Assert the decoded result is equal to the original decimal value
        assertEquals(result, decimalValue);
    }
}
