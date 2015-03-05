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
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.driver.ColumnInfo;

public class ArrayTest {

    @Test
    public void testArrayOfUtf8() throws Exception {
        Schema recordTypeSchema = new Parser().parse(new File("target/test-classes/array/StringUtf8/record.avsc"));

        // prepare ArrayList with elements to test
        List<Utf8> arrayElements = new ArrayList<Utf8>(1);
        arrayElements.add(new Utf8("test string"));

        // construct the GenericArray from the above ArrayList
        Schema recordMemberSchema = recordTypeSchema.getField("recordMember").schema();
        GenericArray<Utf8> avroArray = new GenericData.Array<Utf8>(recordMemberSchema, arrayElements);

        // put the GenericArray in a GenericRecord
        GenericRecord recordType = new GenericData.Record(recordTypeSchema);
        recordType.put(0, avroArray);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/StringUtf8/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(recordType);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { "test string" }, res.get(0));
    }

    @Test
    public void testArrayOfString() throws Exception {
        Schema recordTypeSchema = new Parser().parse(new File("target/test-classes/array/StringUtf8/record.avsc"));

        // prepare ArrayList with elements to test
        List<String> arrayElements = new ArrayList<String>(1);
        arrayElements.add("test string");

        // construct the GenericArray from the above ArrayList
        Schema recordMemberSchema = recordTypeSchema.getField("recordMember").schema();
        GenericArray<String> avroArray = new GenericData.Array<String>(recordMemberSchema, arrayElements);

        // put the GenericArray in a GenericRecord
        GenericRecord recordType = new GenericData.Record(recordTypeSchema);
        recordType.put(0, avroArray);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/StringUtf8/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(recordType);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { "test string" }, res.get(0));
    }

    @Test
    public void testArrayOfInteger() throws Exception {
        Schema recordTypeSchema = new Parser().parse(new File("target/test-classes/array/Integer/record.avsc"));

        int intVal = 12;

        // prepare ArrayList with elements to test
        List<Integer> arrayElements = new ArrayList<Integer>(1);
        arrayElements.add(intVal);

        // construct the GenericArray from the above ArrayList
        Schema recordMemberSchema = recordTypeSchema.getField("recordMember").schema();
        GenericArray<Integer> avroArray = new GenericData.Array<Integer>(recordMemberSchema, arrayElements);

        // put the GenericArray in a GenericRecord
        GenericRecord recordType = new GenericData.Record(recordTypeSchema);
        recordType.put(0, avroArray);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/Integer/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(recordType);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { intVal }, res.get(0));
    }

    @Test
    public void testArrayOfFloat() throws Exception {
        Schema recordTypeSchema = new Parser().parse(new File("target/test-classes/array/Float/record.avsc"));

        float floatVal = 12.34f;

        // prepare ArrayList with elements to test
        List<Float> arrayElements = new ArrayList<Float>(1);
        arrayElements.add(floatVal);

        // construct the GenericArray from the above ArrayList
        Schema recordMemberSchema = recordTypeSchema.getField("recordMember").schema();
        GenericArray<Float> avroArray = new GenericData.Array<Float>(recordMemberSchema, arrayElements);

        // put the GenericArray in a GenericRecord
        GenericRecord recordType = new GenericData.Record(recordTypeSchema);
        recordType.put(0, avroArray);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.FLOAT, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/Float/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(recordType);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { floatVal }, res.get(0));
    }

    @Test
    public void testArrayOfDouble() throws Exception {
        Schema recordTypeSchema = new Parser().parse(new File("target/test-classes/array/Double/record.avsc"));

        double doubleVal = 12.34d;

        // prepare ArrayList with elements to test
        List<Double> arrayElements = new ArrayList<Double>(1);
        arrayElements.add(doubleVal);

        // construct the GenericArray from the above ArrayList
        Schema recordMemberSchema = recordTypeSchema.getField("recordMember").schema();
        GenericArray<Double> avroArray = new GenericData.Array<Double>(recordMemberSchema, arrayElements);

        // put the GenericArray in a GenericRecord
        GenericRecord recordType = new GenericData.Record(recordTypeSchema);
        recordType.put(0, avroArray);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.DOUBLE, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/Double/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(recordType);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { doubleVal }, res.get(0));
    }

    @Test
    public void testArrayOfLong() throws Exception {
        Schema recordTypeSchema = new Parser().parse(new File("target/test-classes/array/Long/record.avsc"));

        long longVal = 12l;

        // prepare ArrayList with elements to test
        List<Long> arrayElements = new ArrayList<Long>(1);
        arrayElements.add(longVal);

        // construct the GenericArray from the above ArrayList
        Schema recordMemberSchema = recordTypeSchema.getField("recordMember").schema();
        GenericArray<Long> avroArray = new GenericData.Array<Long>(recordMemberSchema, arrayElements);

        // put the GenericArray in a GenericRecord
        GenericRecord recordType = new GenericData.Record(recordTypeSchema);
        recordType.put(0, avroArray);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.INT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/Long/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(recordType);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { longVal }, res.get(0));
    }

    @Test
    public void testArrayOfIntRecord() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/array/IntRecord/record.avsc"));
        Schema arraySchema = schema.getField("recordMember").schema();
        Schema arrayMemberSchema = arraySchema.getElementType();

        int intVal = 16;

        // construct int record
        GenericRecord subArrayRecord = new GenericData.Record(arrayMemberSchema);
        subArrayRecord.put(0, intVal);

        // construct array of "int record"
        List<GenericRecord> recordMemberArray = new ArrayList<GenericRecord>(1);
        recordMemberArray.add(subArrayRecord);
        GenericArray<GenericRecord> subArrayElementsGenericArray = new GenericData.Array<GenericRecord>(arraySchema,
                recordMemberArray);

        // construct a record with the above array
        GenericRecord rootRecord = new GenericData.Record(schema);
        rootRecord.put(0, subArrayElementsGenericArray);

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/IntRecord/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(rootRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { intVal }, res.get(0));
    }

    @Test
    public void testMultipleArrayOfStringsBothUsedInConfig() throws Exception {
        /*
         * define a record with 2 separate arrays
         * 
         * fill both arrays with data
         * 
         * have both entries of the arrays in the config file
         */
        Schema schema = new Parser().parse(new File("target/test-classes/array/MultipleArrayOfStrings/schema.avsc"));
        Schema arrayRecordSchema1 = schema.getField("arrayRecord1").schema();
        Schema arrayElementSchema1 = arrayRecordSchema1.getField("arrayElement1").schema();
        Schema arrayRecordSchema2 = schema.getField("arrayRecord2").schema();
        Schema arrayElementSchema2 = arrayRecordSchema2.getField("arrayElement2").schema();

        // ///////

        List<String> arrayElements1 = new ArrayList<String>(1);
        arrayElements1.add("test string 1");

        GenericRecord arrayRecord1 = new GenericData.Record(arrayRecordSchema1);
        GenericArray<String> avroArray1 = new GenericData.Array<String>(arrayElementSchema1, arrayElements1);
        arrayRecord1.put(0, avroArray1);

        GenericRecord firstRecord = new GenericData.Record(schema);
        firstRecord.put(0, arrayRecord1);

        // ///////

        List<String> arrayElements2 = new ArrayList<String>(1);
        arrayElements2.add("test string 2");

        GenericRecord arrayRecord2 = new GenericData.Record(arrayRecordSchema2);
        GenericArray<String> avroArray2 = new GenericData.Array<String>(arrayElementSchema2, arrayElements2);
        arrayRecord2.put(0, avroArray2);

        firstRecord.put(1, arrayRecord2);

        // ///////

        ColumnInfo[] colInfo = new ColumnInfo[2];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.VARSTRING, 0, 0);
        colInfo[1] = AdaptorTestUtils.constructColumnInfo("id2", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File(
                "target/test-classes/array/MultipleArrayOfStrings/avroBothArraysUsed.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(firstRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { "test string 1", "test string 2" }, res.get(0));
    }

    @Test
    public void testMultipleArrayOfStringsOnlyOneUsedInConfig() throws Exception {
        /*
         * define a record with 2 arrays
         * 
         * fill first array with one element
         * 
         * fill second array with two elements
         * 
         * mapping file uses only the first array
         * 
         * ensure 1 row is returned (unused array of 2 elements is not unfolded)
         */
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        Schema schema = new Parser().parse(new File("target/test-classes/array/MultipleArrayOfStrings/schema.avsc"));
        Schema arrayRecordSchema1 = schema.getField("arrayRecord1").schema();
        Schema arrayElementSchema1 = arrayRecordSchema1.getField("arrayElement1").schema();
        Schema arrayRecordSchema2 = schema.getField("arrayRecord2").schema();
        Schema arrayElementSchema2 = arrayRecordSchema2.getField("arrayElement2").schema();

        List<String> arrayElements1 = new ArrayList<String>(1);
        arrayElements1.add("test string 1");
        GenericRecord arrayRecord1 = new GenericData.Record(arrayRecordSchema1);
        GenericArray<String> avroArray1 = new GenericData.Array<String>(arrayElementSchema1, arrayElements1);
        arrayRecord1.put(0, avroArray1);
        GenericRecord firstRecord = new GenericData.Record(schema);
        firstRecord.put(0, arrayRecord1);

        List<String> arrayElements2 = new ArrayList<String>(2);
        arrayElements2.add("test string 2");
        arrayElements2.add("test string 3");
        GenericRecord arrayRecord2 = new GenericData.Record(arrayRecordSchema2);
        GenericArray<String> avroArray2 = new GenericData.Array<String>(arrayElementSchema2, arrayElements2);
        arrayRecord2.put(0, avroArray2);
        firstRecord.put(1, arrayRecord2);

        AvroAdaptor decoder = new AvroAdaptor(new File(
                "target/test-classes/array/MultipleArrayOfStrings/avroOnlyOneArrayUsed.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(firstRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { "test string 1" }, res.get(0));

        /**
         * Now create and test a second record
         */

        GenericRecord secondRecord = new GenericData.Record(schema);
        secondRecord.put(0, arrayRecord1);
        secondRecord.put(1, arrayRecord2);

        List<Object[]> res2 = decoder.convertRecord(secondRecord);
        assertEquals("resulting list size", 1, res2.size());
        assertArrayEquals("resulting item", new Object[] { "test string 1" }, res2.get(0));
    }

    /**
     * define a record with: a record + an array
     * 
     * fill first record with string value
     * 
     * fill second array with two elements
     * 
     * mapping file uses both the record and the array
     * 
     * ensure 2 row is returned
     * 
     * [{"a",arrayVal1}, {"a", arrayVal2}]
     */
    @Test
    public void testRecordWithRecordAndArray() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[2];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", AdaptorTestUtils.Type.VARSTRING, 0, 0);
        colInfo[1] = AdaptorTestUtils.constructColumnInfo("address", AdaptorTestUtils.Type.VARSTRING, 0, 0);

        Schema schema = new Parser().parse(new File("target/test-classes/array/RecordWithRecordAndArray/schema.avsc"));

        GenericRecord firstRecord = new GenericData.Record(schema);
        firstRecord.put("name", "test name");

        Schema addressArraySchema = schema.getField("addressArray").schema();
        List<String> addressElements = new ArrayList<String>(2);
        addressElements.add("street 1");
        addressElements.add("street 2");
        GenericArray<String> avroAddressArray = new GenericData.Array<String>(addressArraySchema, addressElements);

        firstRecord.put("addressArray", avroAddressArray);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/array/RecordWithRecordAndArray/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(firstRecord);
        assertEquals("resulting list size", 2, res.size());
        assertArrayEquals("resulting item", new Object[] { "test name", "street 1" }, res.get(0));
        assertArrayEquals("resulting item", new Object[] { "test name", "street 2" }, res.get(1));
    }
}
