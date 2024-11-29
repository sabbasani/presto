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
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.google.common.base.CharMatcher;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.JsonStringArrayList;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ArrowPageUtils
{
    private ArrowPageUtils()
    {
    }

    public static Block buildBlockFromVector(FieldVector vector, Type type)
    {
        if (vector instanceof BitVector) {
            return buildBlockFromBitVector((BitVector) vector, type);
        }
        else if (vector instanceof TinyIntVector) {
            return buildBlockFromTinyIntVector((TinyIntVector) vector, type);
        }
        else if (vector instanceof IntVector) {
            return buildBlockFromIntVector((IntVector) vector, type);
        }
        else if (vector instanceof SmallIntVector) {
            return buildBlockFromSmallIntVector((SmallIntVector) vector, type);
        }
        else if (vector instanceof BigIntVector) {
            return buildBlockFromBigIntVector((BigIntVector) vector, type);
        }
        else if (vector instanceof DecimalVector) {
            return buildBlockFromDecimalVector((DecimalVector) vector, type);
        }
        else if (vector instanceof NullVector) {
            return buildBlockFromNullVector((NullVector) vector, type);
        }
        else if (vector instanceof TimeStampMicroVector) {
            return buildBlockFromTimeStampMicroVector((TimeStampMicroVector) vector, type);
        }
        else if (vector instanceof TimeStampMilliVector) {
            return buildBlockFromTimeStampMilliVector((TimeStampMilliVector) vector, type);
        }
        else if (vector instanceof Float4Vector) {
            return buildBlockFromFloat4Vector((Float4Vector) vector, type);
        }
        else if (vector instanceof Float8Vector) {
            return buildBlockFromFloat8Vector((Float8Vector) vector, type);
        }
        else if (vector instanceof VarCharVector) {
            if (type instanceof CharType) {
                return buildCharTypeBlockFromVarcharVector((VarCharVector) vector, type);
            }
            else if (type instanceof TimeType) {
                return buildTimeTypeBlockFromVarcharVector((VarCharVector) vector, type);
            }
            else {
                return buildBlockFromVarCharVector((VarCharVector) vector, type);
            }
        }
        else if (vector instanceof VarBinaryVector) {
            return buildBlockFromVarBinaryVector((VarBinaryVector) vector, type);
        }
        else if (vector instanceof DateDayVector) {
            return buildBlockFromDateDayVector((DateDayVector) vector, type);
        }
        else if (vector instanceof DateMilliVector) {
            return buildBlockFromDateMilliVector((DateMilliVector) vector, type);
        }
        else if (vector instanceof TimeMilliVector) {
            return buildBlockFromTimeMilliVector((TimeMilliVector) vector, type);
        }
        else if (vector instanceof TimeSecVector) {
            return buildBlockFromTimeSecVector((TimeSecVector) vector, type);
        }
        else if (vector instanceof TimeStampSecVector) {
            return buildBlockFromTimeStampSecVector((TimeStampSecVector) vector, type);
        }
        else if (vector instanceof TimeMicroVector) {
            return buildBlockFromTimeMicroVector((TimeMicroVector) vector, type);
        }
        else if (vector instanceof TimeStampMilliTZVector) {
            return buildBlockFromTimeMilliTZVector((TimeStampMilliTZVector) vector, type);
        }
        else if (vector instanceof ListVector) {
            return buildBlockFromListVector((ListVector) vector, type);
        }

        throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass().getSimpleName());
    }

    public static Block buildBlockFromEncodedVector(FieldVector encodedVector, Dictionary dictionary)
    {
        // Validate inputs
        if (encodedVector == null || dictionary == null) {
            throw new IllegalArgumentException("Both encodedVector and dictionary must be non-null.");
        }

        // Decode the encoded vector using the dictionary
        ValueVector decodedVector = DictionaryEncoder.decode(encodedVector, dictionary);

        // Create a BlockBuilder for the decoded vector's data type
        Type prestoType = getPrestoTypeFromArrowType(decodedVector.getField().getType());
        BlockBuilder builder = prestoType.createBlockBuilder(null, decodedVector.getValueCount());

        // Populate the block dynamically based on vector type
        for (int i = 0; i < decodedVector.getValueCount(); i++) {
            if (decodedVector.isNull(i)) {
                builder.appendNull(); // Append null for null values
            }
            else {
                // Handle based on vector type
                appendValueToBlock(decodedVector, i, prestoType, builder);
            }
        }

        return builder.build();
    }

    private static Type getPrestoTypeFromArrowType(ArrowType arrowType)
    {
        if (arrowType instanceof ArrowType.Utf8) {
            return VarcharType.VARCHAR;
        }
        else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() == 8 || intType.getBitWidth() == 16 || intType.getBitWidth() == 32) {
                return IntegerType.INTEGER;
            }
            else if (intType.getBitWidth() == 64) {
                return BigintType.BIGINT;
            }
        }
        else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
            FloatingPointPrecision precision = fpType.getPrecision();

            if (precision == FloatingPointPrecision.SINGLE) { // 32-bit float
                return RealType.REAL;
            }
            else if (precision == FloatingPointPrecision.DOUBLE) { // 64-bit float
                return DoubleType.DOUBLE;
            }
            else {
                throw new UnsupportedOperationException("Unsupported FloatingPoint precision: " + precision);
            }
        }
        else if (arrowType instanceof ArrowType.Bool) {
            return BooleanType.BOOLEAN;
        }
        else if (arrowType instanceof ArrowType.Binary) {
            return VarbinaryType.VARBINARY;
        }
        else if (arrowType instanceof ArrowType.Decimal) {
            return DecimalType.createDecimalType();
        }
        throw new UnsupportedOperationException("Unsupported ArrowType: " + arrowType);
    }

    private static void appendValueToBlock(ValueVector vector, int index, Type prestoType, BlockBuilder builder)
    {
        if (vector instanceof VarCharVector) {
            VarCharVector varCharVector = (VarCharVector) vector;
            byte[] valueBytes = varCharVector.get(index);
            prestoType.writeSlice(builder, Slices.utf8Slice(new String(valueBytes, StandardCharsets.UTF_8)));
        }
        else if (vector instanceof IntVector) {
            IntVector intVector = (IntVector) vector;
            prestoType.writeLong(builder, intVector.get(index));
        }
        else if (vector instanceof BigIntVector) {
            BigIntVector bigIntVector = (BigIntVector) vector;
            prestoType.writeLong(builder, bigIntVector.get(index));
        }
        else if (vector instanceof Float4Vector) {
            Float4Vector floatVector = (Float4Vector) vector;
            prestoType.writeLong(builder, Float.floatToRawIntBits(floatVector.get(index)));
        }
        else if (vector instanceof Float8Vector) {
            Float8Vector doubleVector = (Float8Vector) vector;
            prestoType.writeDouble(builder, doubleVector.get(index));
        }
        else if (vector instanceof BitVector) {
            BitVector bitVector = (BitVector) vector;
            prestoType.writeBoolean(builder, bitVector.get(index) == 1);
        }
        else if (vector instanceof VarBinaryVector) {
            VarBinaryVector binaryVector = (VarBinaryVector) vector;
            byte[] valueBytes = binaryVector.get(index);
            prestoType.writeSlice(builder, Slices.wrappedBuffer(valueBytes));
        }
        else {
            throw new UnsupportedOperationException("Unsupported vector type: " + vector.getClass());
        }
    }

    public static Block buildBlockFromTimeMilliTZVector(TimeStampMilliTZVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Type must be a TimestampType for TimeStampMilliTZVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromBitVector(BitVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeBoolean(builder, vector.get(i) == 1);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromIntVector(IntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromSmallIntVector(SmallIntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromTinyIntVector(TinyIntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromBigIntVector(BigIntVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromDecimalVector(DecimalVector vector, Type type)
    {
        if (!(type instanceof DecimalType)) {
            throw new IllegalArgumentException("Type must be a DecimalType for DecimalVector");
        }

        DecimalType decimalType = (DecimalType) type;
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                BigDecimal decimal = vector.getObject(i); // Get the BigDecimal value
                if (decimalType.isShort()) {
                    builder.writeLong(decimal.unscaledValue().longValue());
                }
                else {
                    Slice slice = Decimals.encodeScaledValue(decimal);
                    decimalType.writeSlice(builder, slice, 0, slice.length());
                }
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromNullVector(NullVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            builder.appendNull();
        }
        return builder.build();
    }

    public static Block buildBlockFromTimeStampMicroVector(TimeStampMicroVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Expected TimestampType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long micros = vector.get(i);
                long millis = TimeUnit.MICROSECONDS.toMillis(micros);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromTimeStampMilliVector(TimeStampMilliVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Expected TimestampType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromFloat8Vector(Float8Vector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeDouble(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromFloat4Vector(Float4Vector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                int intBits = Float.floatToIntBits(vector.get(i));
                type.writeLong(builder, intBits);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromVarBinaryVector(VarBinaryVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                byte[] value = vector.get(i);
                type.writeSlice(builder, Slices.wrappedBuffer(value));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromVarCharVector(VarCharVector vector, Type type)
    {
        if (!(type instanceof VarcharType)) {
            throw new IllegalArgumentException("Expected VarcharType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String value = new String(vector.get(i), StandardCharsets.UTF_8);
                type.writeSlice(builder, Slices.utf8Slice(value));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromDateDayVector(DateDayVector vector, Type type)
    {
        if (!(type instanceof DateType)) {
            throw new IllegalArgumentException("Expected DateType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, vector.get(i));
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromDateMilliVector(DateMilliVector vector, Type type)
    {
        if (!(type instanceof DateType)) {
            throw new IllegalArgumentException("Expected DateType but got " + type.getClass().getName());
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                DateType dateType = (DateType) type;
                long days = TimeUnit.MILLISECONDS.toDays(vector.get(i));
                dateType.writeLong(builder, days);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromTimeSecVector(TimeSecVector vector, Type type)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimeSecVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                int value = vector.get(i);
                long millis = TimeUnit.SECONDS.toMillis(value);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromTimeMilliVector(TimeMilliVector vector, Type type)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimeSecVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long millis = vector.get(i);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromTimeMicroVector(TimeMicroVector vector, Type type)
    {
        if (!(type instanceof TimeType)) {
            throw new IllegalArgumentException("Type must be a TimeType for TimemicroVector");
        }
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long value = vector.get(i);
                long micro = TimeUnit.MICROSECONDS.toMillis(value);
                type.writeLong(builder, micro);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromTimeStampSecVector(TimeStampSecVector vector, Type type)
    {
        if (!(type instanceof TimestampType)) {
            throw new IllegalArgumentException("Type must be a TimestampType for TimeStampSecVector");
        }

        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                long value = vector.get(i);
                long millis = TimeUnit.SECONDS.toMillis(value);
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    public static Block buildCharTypeBlockFromVarcharVector(VarCharVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String value = new String(vector.get(i), StandardCharsets.UTF_8);
                type.writeSlice(builder, Slices.utf8Slice(CharMatcher.is(' ').trimTrailingFrom(value)));
            }
        }
        return builder.build();
    }

    public static Block buildTimeTypeBlockFromVarcharVector(VarCharVector vector, Type type)
    {
        BlockBuilder builder = type.createBlockBuilder(null, vector.getValueCount());
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                builder.appendNull();
            }
            else {
                String timeString = new String(vector.get(i), StandardCharsets.UTF_8);
                LocalTime time = LocalTime.parse(timeString);
                long millis = Duration.between(LocalTime.MIN, time).toMillis();
                type.writeLong(builder, millis);
            }
        }
        return builder.build();
    }

    public static Block buildBlockFromListVector(ListVector vector, Type type)
    {
        if (!(type instanceof ArrayType)) {
            throw new IllegalArgumentException("Type must be an ArrayType for ListVector");
        }

        ArrayType arrayType = (ArrayType) type;
        Type elementType = arrayType.getElementType();
        BlockBuilder arrayBuilder = type.createBlockBuilder(null, vector.getValueCount());

        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) {
                arrayBuilder.appendNull();
            }
            else {
                BlockBuilder elementBuilder = arrayBuilder.beginBlockEntry();
                UnionListReader reader = vector.getReader();
                reader.setPosition(i);

                while (reader.next()) {
                    Object value = reader.readObject();
                    if (value == null) {
                        elementBuilder.appendNull();
                    }
                    else {
                        appendValueToBuilder(elementType, elementBuilder, value);
                    }
                }
                arrayBuilder.closeEntry();
            }
        }
        return arrayBuilder.build();
    }

    public static void appendValueToBuilder(Type type, BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
            return;
        }

        if (type instanceof VarcharType) {
            handleVarcharType(type, builder, value);
        }
        else if (type instanceof SmallintType) {
            handleSmallintType(type, builder, value);
        }
        else if (type instanceof TinyintType) {
            handleTinyintType(type, builder, value);
        }
        else if (type instanceof BigintType) {
            handleBigintType(type, builder, value);
        }
        else if (type instanceof IntegerType) {
            handleIntegerType(type, builder, value);
        }
        else if (type instanceof DoubleType) {
            handleDoubleType(type, builder, value);
        }
        else if (type instanceof BooleanType) {
            handleBooleanType(type, builder, value);
        }
        else if (type instanceof DecimalType) {
            handleDecimalType((DecimalType) type, builder, value);
        }
        else if (type instanceof ArrayType) {
            handleArrayType((ArrayType) type, builder, value);
        }
        else if (type instanceof RowType) {
            handleRowType((RowType) type, builder, value);
        }
        else if (type instanceof DateType) {
            handleDateType(type, builder, value);
        }
        else if (type instanceof TimestampType) {
            handleTimestampType(type, builder, value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    public static void handleVarcharType(Type type, BlockBuilder builder, Object value)
    {
        Slice slice = Slices.utf8Slice(value.toString());
        type.writeSlice(builder, slice);
    }

    public static void handleSmallintType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof Number) {
            type.writeLong(builder, ((Number) value).shortValue());
        }
        else if (value instanceof JsonStringArrayList) {
            for (Object obj : (JsonStringArrayList) value) {
                try {
                    short shortValue = Short.parseShort(obj.toString());
                    type.writeLong(builder, shortValue);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number format in JsonStringArrayList for SmallintType: " + obj, e);
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported type for SmallintType: " + value.getClass());
        }
    }

    public static void handleTinyintType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof Number) {
            type.writeLong(builder, ((Number) value).byteValue());
        }
        else if (value instanceof JsonStringArrayList) {
            for (Object obj : (JsonStringArrayList) value) {
                try {
                    byte byteValue = Byte.parseByte(obj.toString());
                    type.writeLong(builder, byteValue);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number format in JsonStringArrayList for TinyintType: " + obj, e);
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported type for TinyintType: " + value.getClass());
        }
    }

    public static void handleBigintType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof Long) {
            type.writeLong(builder, (Long) value);
        }
        else if (value instanceof Integer) {
            type.writeLong(builder, ((Integer) value).longValue());
        }
        else if (value instanceof JsonStringArrayList) {
            for (Object obj : (JsonStringArrayList) value) {
                try {
                    long longValue = Long.parseLong(obj.toString());
                    type.writeLong(builder, longValue);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number format in JsonStringArrayList: " + obj, e);
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported type for BigintType: " + value.getClass());
        }
    }

    public static void handleIntegerType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof Integer) {
            type.writeLong(builder, (Integer) value);
        }
        else if (value instanceof JsonStringArrayList) {
            for (Object obj : (JsonStringArrayList) value) {
                try {
                    int intValue = Integer.parseInt(obj.toString());
                    type.writeLong(builder, intValue);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number format in JsonStringArrayList: " + obj, e);
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported type for IntegerType: " + value.getClass());
        }
    }

    public static void handleDoubleType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof Double) {
            type.writeDouble(builder, (Double) value);
        }
        else if (value instanceof Float) {
            type.writeDouble(builder, ((Float) value).doubleValue());
        }
        else if (value instanceof JsonStringArrayList) {
            for (Object obj : (JsonStringArrayList) value) {
                try {
                    double doubleValue = Double.parseDouble(obj.toString());
                    type.writeDouble(builder, doubleValue);
                }
                catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid number format in JsonStringArrayList: " + obj, e);
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported type for DoubleType: " + value.getClass());
        }
    }

    public static void handleBooleanType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof Boolean) {
            type.writeBoolean(builder, (Boolean) value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type for BooleanType: " + value.getClass());
        }
    }

    public static void handleDecimalType(DecimalType type, BlockBuilder builder, Object value)
    {
        if (value instanceof BigDecimal) {
            BigDecimal decimalValue = (BigDecimal) value;
            if (type.isShort()) {
                // Handle ShortDecimalType
                long unscaledValue = decimalValue.unscaledValue().longValue();
                type.writeLong(builder, unscaledValue);
            }
            else {
                // Handle LongDecimalType
                Slice slice = Decimals.encodeScaledValue(decimalValue);
                type.writeSlice(builder, slice);
            }
        }
        else if (value instanceof Long) {
            // Direct handling for ShortDecimalType using long
            if (type.isShort()) {
                type.writeLong(builder, (Long) value);
            }
            else {
                throw new IllegalArgumentException("Long value is not supported for LongDecimalType.");
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported type for DecimalType: " + value.getClass());
        }
    }

    public static void handleArrayType(ArrayType type, BlockBuilder builder, Object value)
    {
        Type elementType = type.getElementType();
        BlockBuilder arrayBuilder = builder.beginBlockEntry();
        for (Object element : (Iterable<?>) value) {
            appendValueToBuilder(elementType, arrayBuilder, element);
        }
        builder.closeEntry();
    }

    public static void handleRowType(RowType type, BlockBuilder builder, Object value)
    {
        List<Object> rowValues = (List<Object>) value;
        BlockBuilder rowBuilder = builder.beginBlockEntry();
        List<RowType.Field> fields = type.getFields();
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = fields.get(i).getType();
            appendValueToBuilder(fieldType, rowBuilder, rowValues.get(i));
        }
        builder.closeEntry();
    }

    public static void handleDateType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof java.sql.Date || value instanceof java.time.LocalDate) {
            int daysSinceEpoch = (int) (value instanceof java.sql.Date
                    ? ((java.sql.Date) value).toLocalDate().toEpochDay()
                    : ((java.time.LocalDate) value).toEpochDay());
            type.writeLong(builder, daysSinceEpoch);
        }
        else {
            throw new IllegalArgumentException("Unsupported type for DateType: " + value.getClass());
        }
    }

    public static void handleTimestampType(Type type, BlockBuilder builder, Object value)
    {
        if (value instanceof java.sql.Timestamp) {
            long millis = ((java.sql.Timestamp) value).getTime();
            type.writeLong(builder, millis);
        }
        else if (value instanceof java.time.Instant) {
            long millis = ((java.time.Instant) value).toEpochMilli();
            type.writeLong(builder, millis);
        }
        else if (value instanceof Long) { // Handle long epoch milliseconds directly
            type.writeLong(builder, (Long) value);
        }
        else {
            throw new IllegalArgumentException("Unsupported type for TimestampType: " + value.getClass());
        }
    }
}
