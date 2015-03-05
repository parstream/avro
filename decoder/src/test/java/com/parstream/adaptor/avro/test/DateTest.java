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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ParstreamDate;
import com.parstream.driver.ParstreamShortDate;
import com.parstream.driver.ParstreamTime;
import com.parstream.driver.ParstreamTimestamp;

public class DateTest {

    @Test
    public void testIntToShortDate() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/date/IntToShortDate/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        GregorianCalendar cal = new GregorianCalendar();
        newRecord.put(0, (int) (cal.getTimeInMillis() / 1000L));

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.SHORTDATE, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/date/IntToShortDate/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertEquals("size of elements in object[]", 1, res.get(0).length);
        assertTrue("check short date compatability", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamShortDate.class, new ParstreamShortDate(cal), (ParstreamShortDate) res.get(0)[0]));
    }

    @Test
    public void testLongToShortDate() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/date/LongToShortDate/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        GregorianCalendar cal = new GregorianCalendar();
        newRecord.put(0, cal.getTimeInMillis());

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.SHORTDATE, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/date/LongToShortDate/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertEquals("array length in result", 1, res.get(0).length);
        assertTrue("validate parstream short date equivalency", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamShortDate.class, new ParstreamShortDate(cal), (ParstreamShortDate) res.get(0)[0]));
    }

    @Test
    public void testLongToDate() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/date/LongToDate/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        GregorianCalendar cal = new GregorianCalendar();
        newRecord.put(0, cal.getTimeInMillis());

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.DATE, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/date/LongToDate/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertEquals("resulting object[] length", 1, res.get(0).length);
        assertTrue("resulting parstream date objects identical", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamDate.class, new ParstreamDate(cal), (ParstreamDate) res.get(0)[0]));
    }

    @Test
    public void testLongToTime() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/date/LongToTime/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        GregorianCalendar cal = new GregorianCalendar();
        newRecord.put(0, cal.getTimeInMillis());

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.TIME, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/date/LongToTime/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertEquals("resulting Object[] length", 1, res.get(0).length);
        assertTrue("resulting Parstream Time objects identical", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamTime.class, new ParstreamTime(cal), (ParstreamTime) res.get(0)[0]));
    }

    @Test
    public void testLongToTimestamp() throws Exception {
        Schema schema = new Parser().parse(new File("target/test-classes/date/LongToTimestamp/record.avsc"));
        GenericRecord newRecord = new GenericData.Record(schema);
        GregorianCalendar cal = new GregorianCalendar();
        newRecord.put(0, cal.getTimeInMillis());

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("id", AdaptorTestUtils.Type.TIMESTAMP, 0, 0);

        AvroAdaptor decoder = new AvroAdaptor(new File("target/test-classes/date/LongToTimestamp/avro.ini"), colInfo);
        List<Object[]> res = decoder.convertRecord(newRecord);
        assertEquals("resulting list size", 1, res.size());
        assertEquals("resulting object[] length", 1, res.get(0).length);
        assertTrue("resulting ParstreamTimestamp", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamTimestamp.class, new ParstreamTimestamp(cal), (ParstreamTimestamp) res.get(0)[0]));
    }
}
