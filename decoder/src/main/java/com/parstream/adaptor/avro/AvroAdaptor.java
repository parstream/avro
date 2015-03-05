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
package com.parstream.adaptor.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ParstreamDate;
import com.parstream.driver.ParstreamException;
import com.parstream.driver.ParstreamShortDate;
import com.parstream.driver.ParstreamTime;
import com.parstream.driver.ParstreamTimestamp;

/**
 * This adaptor converts Avro records into ParStream rows. A single Avro record
 * is decoded into a list of ParStream rows. The produced rows can be imported
 * into a ParStream database using the ParStream Java Streaming Import
 * Interface.
 * <p>
 * The adaptor requires a configuration file, which defines exactly which Avro
 * fields are mapped to which columns in a ParStream table. This file is read as
 * java Properties files. <br>
 * The property keys defines the name of the table column and the property
 * values defines the absolute paths of the used Avro field schema name. The
 * following is a sample line in the configuration file.
 * 
 * <pre>
 * column.psColumnName = recordName.fieldName2
 * </pre>
 */
public class AvroAdaptor {

    private static final String ERROR_NULL_COLUMN_INFO = "ColumnInfo must not be null";
    private static final String ERROR_NULL_INPUT_STREAM = "config file stream must not be null";
    private static final String ERROR_NULL_MAP_FILE = "config file path must not be null";
    private static final String FIELD_DELIMITER = ".";

    private ColumnInfo[] _columnInfo;
    private Properties _mappingProps;
    private Map<String, Boolean> _columnsInConfigFile;

    /**
     * Creates a new instance of this adaptor.
     * 
     * @param inputStream
     *            the input stream from which the mapping between Avro fields
     *            and ParStream columns is loaded, must not be null
     * @param columnInfo
     *            the ParStream table column information. must not be null. The
     *            column information can be obtained from the ParStream Java
     *            Client API
     * @throws AvroAdaptorException
     *             if parsed mappings file produces no valid mapping entries
     * @throws IOException
     *             when reading from the provided input stream
     */
    public AvroAdaptor(InputStream inputStream, ColumnInfo[] columnInfo) throws AvroAdaptorException, IOException {
        assert inputStream != null : ERROR_NULL_INPUT_STREAM;
        assert columnInfo != null : ERROR_NULL_COLUMN_INFO;

        initialize(inputStream, columnInfo);
    }

    /**
     * Creates a new instance of this adaptor.
     * 
     * @param configFile
     *            the file from which the mapping between Avro fields and
     *            ParStream columns is loaded, must not be null
     * @param columnInfo
     *            the ParStream table column information, must not be null. The
     *            column information can be obtained from the ParStream Java
     *            Client API
     * @throws AvroAdaptorException
     *             if parsed mappings file produces no valid mapping entries
     * @throws IOException
     *             when reading from the provided mapping file
     */
    public AvroAdaptor(File configFile, ColumnInfo[] columnInfo) throws AvroAdaptorException, IOException {
        assert configFile != null : ERROR_NULL_MAP_FILE;
        assert columnInfo != null : ERROR_NULL_COLUMN_INFO;

        FileInputStream configInputStream = new FileInputStream(configFile);
        try {
            initialize(configInputStream, columnInfo);
        } catch (IOException e) {
            throw e;
        } finally {
            configInputStream.close();
        }
    }

    /**
     * Converts a single Avro record, into zero or more ParStream rows.
     * 
     * @param record
     *            the input avro record to be converted
     * @return a list of Object[]. Each Object[] represents a single row in a
     *         ParStream table
     * @throws AvroAdaptorException
     *             if an incompatible datatype conversion is encountered
     */
    public List<Object[]> convertRecord(GenericRecord record) throws AvroAdaptorException {
        if (record == null) {
            return new ArrayList<Object[]>(0);
        }

        return createRowSet(parseRecord(record, null));
    }

    private void initialize(InputStream inputStream, ColumnInfo[] columnInfo) throws IOException, AvroAdaptorException {
        _mappingProps = new Properties();
        _mappingProps.load(inputStream);

        if (_mappingProps.size() == 0) {
            throw new AvroAdaptorException("no column mappings specified");
        }

        _columnInfo = columnInfo;
        _columnsInConfigFile = new HashMap<String, Boolean>(_mappingProps.size());
    }

    private Map<String, Object> parseRecord(GenericRecord record, String prefix) throws AvroAdaptorException {
        List<Field> fields = record.getSchema().getFields();
        Map<String, Object> recordData = new HashMap<String, Object>(fields.size());

        String newName;
        if (prefix == null) {
            newName = record.getSchema().getFullName();
        } else {
            newName = prefix + record.getSchema().getFullName();
        }

        for (Field field : fields) {
            Object res = record.get(field.name());
            if (res != null) {
                if (res instanceof Record) {
                    Map<String, Object> nestedRecord = parseRecord((Record) res, newName + FIELD_DELIMITER);
                    if (nestedRecord != null) {
                        recordData.putAll(nestedRecord);
                    }
                } else if (res instanceof Utf8 || res instanceof String) {
                    recordData.put(newName + FIELD_DELIMITER + field.name(), res.toString());
                } else if (res instanceof Boolean) {
                    Integer boolValue;
                    if ((Boolean) res) {
                        boolValue = 1;
                    } else {
                        boolValue = 0;
                    }
                    recordData.put(newName + FIELD_DELIMITER + field.name(), boolValue);
                } else if (res instanceof Integer || res instanceof Long || res instanceof Float
                        || res instanceof Double) {
                    recordData.put(newName + FIELD_DELIMITER + field.name(), res);
                } else if (res instanceof Array) {
                    Map<String, Object> parsedArrayHashMap = parseArray((Array<?>) res, newName + FIELD_DELIMITER
                            + field.name());
                    if (parsedArrayHashMap != null) {
                        recordData.putAll(parsedArrayHashMap);
                    }
                } else {
                    throw new AvroAdaptorException("Unsupported datatype: " + res.getClass());
                }
            }
        }
        return recordData;
    }

    private Map<String, Object> parseArray(Array<?> arr, String name) throws AvroAdaptorException {
        // initialize a new hash map, that will keep track of array
        Map<String, Object> recordData = null;

        if (arr.size() > 0) {
            List<Map<String, Object>> arrayValues = new ArrayList<Map<String, Object>>(arr.size());
            Iterator<?> arrayIterator = arr.iterator();
            while (arrayIterator.hasNext()) {
                Object arrayElement = arrayIterator.next();
                if (arrayElement instanceof Record) {
                    Map<String, Object> arrayRecordValues = parseRecord((Record) arrayElement, name + FIELD_DELIMITER);
                    arrayValues.add(arrayRecordValues);
                } else if (arrayElement instanceof Utf8 || arrayElement instanceof String) {
                    Map<String, Object> arrayRecordValues = new HashMap<String, Object>(1);
                    arrayRecordValues.put(name, arrayElement.toString());
                    arrayValues.add(arrayRecordValues);
                } else if (arrayElement instanceof Integer || arrayElement instanceof Long
                        || arrayElement instanceof Float || arrayElement instanceof Double) {
                    Map<String, Object> arrayRecordValues = new HashMap<String, Object>(1);
                    arrayRecordValues.put(name, arrayElement);
                    arrayValues.add(arrayRecordValues);
                } else {
                    throw new AvroAdaptorException("Unsupported array datatype: " + arrayElement.getClass());
                }
            }

            // once the new hashmap is filled, push it back into recordData
            recordData = new HashMap<String, Object>();
            recordData.put(name, arrayValues);
        }
        return recordData;
    }

    private List<Object[]> createRowSet(Map<String, Object> recordToInsert) throws AvroAdaptorException {
        // The following removes any keys from the hashmap, if they are not used
        // in the mapping file
        for (Object recordKey : recordToInsert.keySet().toArray()) {
            Boolean exists = _columnsInConfigFile.get(recordKey);
            // check if we already scanned the existence of this column in our
            // mapping file
            // true: a check was made with a positive result
            // false: a check was made with a negative result
            if (exists == null) {
                // null: no check was make, make a check and update
                // _columnsInConfigFile accordingly
                boolean found = false;
                for (Object k1 : _mappingProps.values()) {
                    if (k1.toString().startsWith(recordKey.toString())) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    _columnsInConfigFile.put(recordKey.toString(), Boolean.valueOf(true));
                } else {
                    _columnsInConfigFile.put(recordKey.toString(), Boolean.valueOf(false));
                    recordToInsert.remove(recordKey);
                }
            } else if (!exists) {
                recordToInsert.remove(recordKey);
            }
        }

        List<Map<String, Object>> initialList = new ArrayList<Map<String, Object>>();
        initialList.add(recordToInsert);

        List<Map<String, Object>> finalList = new ArrayList<Map<String, Object>>(10);

        while (initialList.size() != 0) {
            Map<String, Object> currentHashMap = initialList.remove(0);
            List<Map<String, Object>> flattened = expandArrays(currentHashMap);
            if (flattened == null) {
                finalList.add(currentHashMap);
            } else {
                initialList.addAll(flattened);
            }
        }
        List<Object[]> listToInsert = new ArrayList<Object[]>(finalList.size());

        for (Map<String, Object> recordData : finalList) {
            Object[] insertValues = new Object[_columnInfo.length];

            for (int i = 0; i < _columnInfo.length; ++i) {
                String psColumnKey = _columnInfo[i].getName();
                String avroKey = _mappingProps.getProperty("column." + psColumnKey);

                if (avroKey == null || "".equals(avroKey.trim())) {
                    insertValues[i] = null;
                } else {
                    Object avroValueObj = recordData.get(avroKey);
                    if (avroValueObj == null) {
                        insertValues[i] = null;
                    } else {
                        // incompatible datatype check
                        switch (_columnInfo[i].getType()) {
                        case UINT8:
                        case UINT16:
                        case UINT32:
                        case UINT64:
                        case INT8:
                        case INT16:
                        case INT32:
                        case INT64:
                        case BITVECTOR8:
                            if (avroValueObj instanceof Integer || avroValueObj instanceof Long) {
                                insertValues[i] = avroValueObj;
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i], avroValueObj);
                            }
                            break;

                        case SHORTDATE:
                            try {
                                insertValues[i] = new ParstreamShortDate((GregorianCalendar) valueToDate(avroValueObj,
                                        psColumnKey, _columnInfo[i]));
                            } catch (ParstreamException e) {
                                throw new AvroAdaptorException(e.getMessage());
                            }
                            break;

                        case DATE:
                            try {
                                insertValues[i] = new ParstreamDate((GregorianCalendar) valueToDate(avroValueObj,
                                        psColumnKey, _columnInfo[i]));
                            } catch (ParstreamException e) {
                                throw new AvroAdaptorException(e.getMessage());
                            }
                            break;

                        case TIME:
                            try {
                                insertValues[i] = new ParstreamTime((GregorianCalendar) valueToDate(avroValueObj,
                                        psColumnKey, _columnInfo[i]));
                            } catch (ParstreamException e) {
                                throw new AvroAdaptorException(e.getMessage());
                            }
                            break;

                        case TIMESTAMP:
                            try {
                                insertValues[i] = new ParstreamTimestamp((GregorianCalendar) valueToDate(avroValueObj,
                                        psColumnKey, _columnInfo[i]));
                            } catch (ParstreamException e) {
                                throw new AvroAdaptorException(e.getMessage());
                            }
                            break;

                        case VARSTRING:
                            if (avroValueObj instanceof String) {
                                insertValues[i] = avroValueObj;
                            } else {
                                insertValues[i] = avroValueObj.toString();
                            }
                            break;

                        case FLOAT:
                            if (avroValueObj instanceof Float) {
                                insertValues[i] = avroValueObj;
                            } else {
                                insertValues[i] = null;
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i], avroValueObj);
                            }
                            break;

                        case DOUBLE:
                            if (avroValueObj instanceof Double) {
                                insertValues[i] = avroValueObj;
                            } else {
                                insertValues[i] = null;
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i], avroValueObj);
                            }
                            break;

                        case BLOB:
                            throw new AvroAdaptorException("ParStream BLOB column type not supported for decoding");

                        default:
                            throw new AvroAdaptorException("Unknown ParStream column type: " + _columnInfo[i].getType());
                        }
                    }
                }
            }
            listToInsert.add(insertValues);
        }

        return listToInsert;
    }

    private void throwIncompatibleTypeException(String psColumnKey, final ColumnInfo columnInfo,
            final Object avroValueObj) throws AvroAdaptorException {
        throw new AvroAdaptorException(
                String.format(
                        "Incompatible datatypes for column (%s). Database type is %s, JAVA type is %s, Value attempted for insertion: %s",
                        psColumnKey, columnInfo.getType().toString(), avroValueObj.getClass(), avroValueObj.toString()));
    }

    private Calendar valueToDate(Object avroValueObj, String psColumnKey, ColumnInfo colInfo)
            throws AvroAdaptorException {
        Calendar cal = new GregorianCalendar();

        if (avroValueObj instanceof Integer) {
            cal.setTimeInMillis(((Integer) avroValueObj).intValue() * 1000L);
        } else if (avroValueObj instanceof Long) {
            cal.setTimeInMillis(((Long) avroValueObj).longValue());
        } else {
            throwIncompatibleTypeException(psColumnKey, colInfo, avroValueObj);
        }
        return cal;
    }

    private List<Map<String, Object>> expandArrays(Map<String, Object> hm) throws AvroAdaptorException {

        /**
         * Gets the key in a HashMap where it has an ArrayList. NULL of not
         * found. The existence of an ArrayList means that, the current HashMap
         * of a GenericRecord has an array that needs to be flattened when
         * creating the ParStream Object[]
         */
        String arrayListItemKey = null;
        for (Entry<String, Object> entry : hm.entrySet()) {
            if (entry.getValue() instanceof ArrayList) {
                arrayListItemKey = entry.getKey();
            }
        }
        if (arrayListItemKey == null) {
            return null;
        }

        // the SuppressWarnings was added, because the item we will retrieve was
        // already checked in
        // arrayListItemKey hasArrayList if it was an instanceof ArrayList
        @SuppressWarnings("unchecked")
        List<Object> nestedItem = (ArrayList<Object>) hm.get(arrayListItemKey);
        hm.remove(arrayListItemKey);

        List<Map<String, Object>> res = new ArrayList<Map<String, Object>>(nestedItem.size());
        if (nestedItem.size() == 0) {
            res.add(hm);
        } else {
            // initialize resulting arraylist, based on the arraylist size
            for (int i = 0; i < nestedItem.size(); ++i) {
                Map<String, Object> newHashMap = new HashMap<String, Object>(hm);

                // retrieve an object from the array list
                Object entryToAdd = nestedItem.get(i);

                if (entryToAdd instanceof HashMap) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> entryToAddHashMap = (HashMap<String, Object>) entryToAdd;
                    for (Entry<String, Object> entry : entryToAddHashMap.entrySet()) {
                        newHashMap.put(entry.getKey(), entry.getValue());
                    }
                } else {
                    throw new AvroAdaptorException(
                            "INTERNAL ERROR: flattenArrayListsInHashMap: Unsupported type: ArrayList elemet not instance of HashMap");
                }
                res.add(newHashMap);
            }
        }
        return res;
    }
}
