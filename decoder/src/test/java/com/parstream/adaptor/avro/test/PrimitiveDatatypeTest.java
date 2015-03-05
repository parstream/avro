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

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.driver.ColumnInfo;

public class PrimitiveDatatypeTest {

    @Test
    public void testNullAvroRecord() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(
                new File("target/test-classes/primitiveDatatype/NullAvroRecord/avro.ini"), colInfo);
        assertEquals("null GenericRecord should return empty list", 0, decoder.convertRecord(null).size());
    }

    @Test
    public void testInt() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/Int/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, new Integer(5));

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/Int/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { 5 }, res.get(0));
    }

    @Test
    public void testLong() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/Long/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, 5l);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT64, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/Long/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { 5l }, res.get(0));
    }

    @Test
    public void testFloat() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/Float/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, 23.45f);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.FLOAT, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/Float/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { 23.45f }, res.get(0));
    }

    @Test
    public void testDouble() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/Double/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, 23.45d);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.DOUBLE, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/Double/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { 23.45d }, res.get(0));
    }

    @Test
    public void testString() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/String/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, "Straße");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/String/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { "Straße" }, res.get(0));
    }

    @Test
    public void testUtf8() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/Utf8/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, new Utf8("Straße"));

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/Utf8/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { "Straße" }, res.get(0));
    }

    @Test
    public void testBooleanTrue() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/Boolean/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, true);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.BITVECTOR8, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/Boolean/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { 1 }, res.get(0));
    }

    @Test
    public void testBooleanFalse() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/primitiveDatatype/Boolean/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, false);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.BITVECTOR8, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/primitiveDatatype/Boolean/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { 0 }, res.get(0));
    }
}
