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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.adaptor.avro.AvroAdaptorException;
import com.parstream.driver.ColumnInfo;

public class ConstructorFailureTest {

    /* Using constructor (File, ColumnInfo[]) */
    @Test
    public void testNullColumnInfo() throws Exception {
        try {
            new AvroAdaptor(new File("target/test-classes/constructor/ValidConfigFileNullColumnInfo/avro.ini"), null);
            fail("null ColInfo should throw exception");
        } catch (AssertionError expected) {
            assertTrue("null ColInfo should throw exception",
                    expected.getMessage().contains("ColumnInfo must not be null"));
        }
    }

    @Test
    public void testNullConfigFile() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        try {
            File mappingFile = null;
            new AvroAdaptor(mappingFile, colInfo);
            fail("null config file path should throw exception");
        } catch (AssertionError expected) {
            assertTrue(expected.getMessage().contains("config file path must not be null"));
        }
    }

    @Test
    public void testNonExistantConfigFilePath() throws AvroAdaptorException, IOException {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);
        File nonExistentFile = new File("slkghskdgh");

        try {
            new AvroAdaptor(nonExistentFile, colInfo);
            fail("non existant config file path should throw exception");
        } catch (FileNotFoundException expected) {
        }
    }

    @Test
    public void testNoColumnMappings() throws IOException {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        try {
            new AvroAdaptor(new File("target/test-classes/constructor/NoColumnMappings/avro.ini"), colInfo);
            fail("no column mappings specified should cause exception");
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().contains("no column mappings specified"));
        }
    }

    @Test
    public void testCommentedColumnMappings() throws IOException {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        try {
            new AvroAdaptor(new File("target/test-classes/constructor/CommentedColumnMappings/avro.ini"), colInfo);
            fail("no column mappings specified should cause exception");
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().contains("no column mappings specified"));
        }
    }

    @Test
    public void testMissingAvroKeyForParstreamKey() throws IOException, AvroAdaptorException {
        Schema schema = new Parser().parse(new File(
                "target/test-classes/constructor/MissingAvroKeyForParstreamKey/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, new Integer(5));

        ColumnInfo[] colInfo = new ColumnInfo[2];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);
        colInfo[1] = AdaptorTestUtils.constructColumnInfo("id2", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File(
                "target/test-classes/constructor/MissingAvroKeyForParstreamKey/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { null, null }, res.get(0));
    }

    @Test
    public void testMissingParstreamKeyForAvroKey() throws IOException, AvroAdaptorException {
        Schema schema = new Parser().parse(new File(
                "target/test-classes/constructor/MissingParstreamKeyForAvroKey/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, new Integer(5));

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        new AvroAdaptor(new File("target/test-classes/constructor/MissingParstreamKeyForAvroKey/avro.ini"), colInfo);
    }

    /* Using constructor (InputStream, ColumnInfo[]) */

    @Test
    public void testNullConfigInputStream() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        try {
            InputStream mappingStream = null;
            new AvroAdaptor(mappingStream, colInfo);
            fail("null config file path should throw exception");
        } catch (AssertionError expected) {
            assertTrue(expected.getMessage().contains("config file stream must not be null"));
        }
    }

    @Test
    public void testNullColumnInfoWithInputStream() throws Exception {
        try {
            InputStream is = ConstructorFailureTest.class
                    .getResourceAsStream("/constructor/ValidConfigFileNullColumnInfo/avro.ini");
            new AvroAdaptor(is, null);
            fail("null ColInfo should throw exception");
        } catch (AssertionError expected) {
            assertTrue("null ColInfo should throw exception",
                    expected.getMessage().contains("ColumnInfo must not be null"));
        }
    }

    @Test
    public void testNoColumnMappingsWithInputStream() throws IOException {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        try {
            new AvroAdaptor(ConstructorFailureTest.class.getResourceAsStream("/constructor/NoColumnMappings/avro.ini"),
                    colInfo);
            fail("no column mappings specified should cause exception");
        } catch (AvroAdaptorException expected) {
            assertTrue(expected.getMessage().contains("no column mappings specified"));
        }
    }

    @Test
    public void testMissingAvroKeyForParstreamKeyWithInputStream() throws IOException, AvroAdaptorException {
        Schema schema = new Parser().parse(new File(
                "target/test-classes/constructor/MissingAvroKeyForParstreamKey/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, new Integer(5));

        ColumnInfo[] colInfo = new ColumnInfo[2];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);
        colInfo[1] = AdaptorTestUtils.constructColumnInfo("id2", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(
                ConstructorFailureTest.class.getResourceAsStream("/constructor/MissingAvroKeyForParstreamKey/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { null, null }, res.get(0));
    }

}
