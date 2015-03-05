/**
 * Copyright 2015 ParStream GmbH
 *
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
package com.parstream.adaptor.avro.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.adaptor.avro.AvroAdaptorException;
import com.parstream.driver.ColumnInfo;

public class InvalidTypeMappingTest {

    @Test
    public void testStringToDate() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/invalidTypeMapping/StringToDate/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, "str");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.DATE, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/invalidTypeMapping/StringToDate/avro.ini"),
                colInfo);
        try {
            decoder.convertRecord(newRecord);
            fail();
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().startsWith("Incompatible datatypes for column"));
        }
    }

    @Test
    public void testStringToUINT32() throws Exception {
        Schema schema = new Parser()
                .parse(new File("target/test-classes/invalidTypeMapping/StringToUINT32/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, "str");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(
                new File("target/test-classes/invalidTypeMapping/StringToUINT32/avro.ini"), colInfo);
        try {
            decoder.convertRecord(newRecord);
            fail();
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().startsWith("Incompatible datatypes for column"));
        }
    }

    @Test
    public void testIntToString() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/invalidTypeMapping/IntToString/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, 3);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/invalidTypeMapping/IntToString/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { "3" }, res.get(0));

    }

    @Test
    public void testStringToFloat() throws Exception {
        Schema schema = new Parser()
                .parse(new File("target/test-classes/invalidTypeMapping/StringToFloat/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, "str");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.FLOAT, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(
                new File("target/test-classes/invalidTypeMapping/StringToFloat/avro.ini"), colInfo);
        try {
            decoder.convertRecord(newRecord);
            fail();
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().startsWith("Incompatible datatypes for column"));
        }
    }

    @Test
    public void testStringToDouble() throws Exception {
        Schema schema = new Parser()
                .parse(new File("target/test-classes/invalidTypeMapping/StringToDouble/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, "str");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.DOUBLE, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(
                new File("target/test-classes/invalidTypeMapping/StringToDouble/avro.ini"), colInfo);
        try {
            decoder.convertRecord(newRecord);
            fail();
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().startsWith("Incompatible datatypes for column"));
        }
    }

    @Test
    public void testUnsupportedParstreamBlob() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/invalidTypeMapping/StringToBlob/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, "str");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.BLOB, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/invalidTypeMapping/StringToBlob/avro.ini"),
                colInfo);
        try {
            decoder.convertRecord(newRecord);
            fail();
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().startsWith("ParStream BLOB column type not supported for decoding"));
        }
    }
}
