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
package io.trino.decoder;

import com.google.common.collect.ImmutableSet;
import io.trino.decoder.csv.CsvRowDecoderFactory;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import static io.trino.decoder.util.DecoderTestUtil.checkIsNull;
import static io.trino.decoder.util.DecoderTestUtil.checkValue;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Collections.emptyMap;
import static org.testng.Assert.assertEquals;

public class TestCsvDecoder
{
    private static final CsvRowsDecoderFactory DECODER_FACTORY = new CsvRowsDecoderFactory();

    @Test
    public void testSimple()
    {
        String csv = "\"row 1\",row2,\"row3\",100,\"200\",300,4.5";

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", createVarcharType(2), "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", createVarcharType(10), "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", createVarcharType(10), "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BigintType.BIGINT, "3", null, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle(4, "row5", BigintType.BIGINT, "4", null, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle(5, "row6", BigintType.BIGINT, "5", null, null, false, false, false);
        DecoderTestColumnHandle row7 = new DecoderTestColumnHandle(6, "row7", DoubleType.DOUBLE, "6", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4, row5, row6, row7);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row1, "ro");
        checkValue(decodedRow, row2, "row2");
        checkValue(decodedRow, row3, "row3");
        checkValue(decodedRow, row4, 100);
        checkValue(decodedRow, row5, 200);
        checkValue(decodedRow, row6, 300);
        checkValue(decodedRow, row7, 4.5d);
    }

    @Test
    public void testBoolean()
    {
        String csv = "True,False,0,1,\"0\",\"1\",\"true\",\"false\"";

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", BooleanType.BOOLEAN, "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", BooleanType.BOOLEAN, "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", BooleanType.BOOLEAN, "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BooleanType.BOOLEAN, "3", null, null, false, false, false);
        DecoderTestColumnHandle row5 = new DecoderTestColumnHandle(4, "row5", BooleanType.BOOLEAN, "4", null, null, false, false, false);
        DecoderTestColumnHandle row6 = new DecoderTestColumnHandle(5, "row6", BooleanType.BOOLEAN, "5", null, null, false, false, false);
        DecoderTestColumnHandle row7 = new DecoderTestColumnHandle(6, "row7", BooleanType.BOOLEAN, "6", null, null, false, false, false);
        DecoderTestColumnHandle row8 = new DecoderTestColumnHandle(7, "row8", BooleanType.BOOLEAN, "7", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4, row5, row6, row7, row8);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkValue(decodedRow, row1, true);
        checkValue(decodedRow, row2, false);
        checkValue(decodedRow, row3, false);
        checkValue(decodedRow, row4, false);
        checkValue(decodedRow, row5, false);
        checkValue(decodedRow, row6, false);
        checkValue(decodedRow, row7, true);
        checkValue(decodedRow, row8, false);
    }

    @Test
    public void testNulls()
    {
        String csv = ",,,";

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle(0, "row1", createVarcharType(10), "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle(1, "row2", BigintType.BIGINT, "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle(2, "row3", DoubleType.DOUBLE, "2", null, null, false, false, false);
        DecoderTestColumnHandle row4 = new DecoderTestColumnHandle(3, "row4", BooleanType.BOOLEAN, "3", null, null, false, false, false);

        Set<DecoderColumnHandle> columns = ImmutableSet.of(row1, row2, row3, row4);
        RowDecoder rowDecoder = DECODER_FACTORY.create(emptyMap(), columns);

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = rowDecoder.decodeRow(csv.getBytes(StandardCharsets.UTF_8))
                .orElseThrow(AssertionError::new);

        assertEquals(decodedRow.size(), columns.size());

        checkIsNull(decodedRow, row1);
        checkIsNull(decodedRow, row2);
        checkIsNull(decodedRow, row3);
        checkIsNull(decodedRow, row4);
    }
}