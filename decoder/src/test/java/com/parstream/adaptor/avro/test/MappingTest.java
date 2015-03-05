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
import org.junit.Test;

import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.driver.ColumnInfo;

public class MappingTest {

    /**
     * This tests when the mapping entry has a void avro key
     */
    @Test
    public void testVoidAvroKey() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/mapping/IntMissingAvroKey/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, new Integer(5));

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/mapping/IntMissingAvroKey/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { null }, res.get(0));
    }

    /**
     * This tests when there is no mapping entry for a Parstream column
     */
    @Test
    public void testUnknownParstreamColumn() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/mapping/IntMissingMappingEntry/schema.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        newRecord.put(0, new Integer(5));

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.UINT32, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/mapping/IntMissingMappingEntry/avro.ini"),
                colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertArrayEquals("resulting item", new Object[] { null }, res.get(0));
    }
}
