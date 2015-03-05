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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.parstream.adaptor.avro.AvroAdaptor;
import com.parstream.adaptor.avro.AvroAdaptorException;
import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ParstreamConnection;

/**
 * This is a Java example on how to use the ParStream Avro Adaptor.
 */

public class AvroAdaptorExample {

    public static void main(String[] args) throws Exception {
        System.out.println("Start avro adaptor tutorial");
        String host;
        String port;
        String avroConfigPath = null;
        String avroDataPath = null;
        String username = null;
        String password = null;
        String tableName = "MyTable";

        if (args.length >= 2) {
            host = args[0];
            port = args[1];
            if (args.length == 4) {
                avroConfigPath = args[2];
                avroDataPath = args[3];
            }
        } else {
            throw new Exception("Insufficient number of runtime arguments. Requiest at least 2: <host> <port>");
        }

        File configFile;
        if (avroConfigPath != null) {
            configFile = new File(avroConfigPath);
        } else {
            configFile = getFileFromResources("avro.config");
        }

        File avroFile;
        if (avroDataPath != null) {
            avroFile = new File(avroDataPath);
        } else {
            avroFile = getFileFromResources("data.avro");
        }

        ParstreamConnection parstreamConn = new ParstreamConnection();
        parstreamConn.createHandle();
        parstreamConn.setTimeout(20000);
        parstreamConn.connect(host, port, username, password);
        System.out.println("Connected to ParStream at " + host + ":" + port);
        ColumnInfo[] columnInfo = parstreamConn.listImportColumns(tableName);

        AvroAdaptor decoder = new AvroAdaptor(configFile, columnInfo);

        DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(avroFile, userDatumReader);
        GenericRecord user = null;
        parstreamConn.prepareInsert(tableName);
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            try {
                List<Object[]> objList = decoder.convertRecord(user);
                for (Object[] objArr : objList) {
                    parstreamConn.rawInsert(objArr);
                    System.out.println("Inserted row into ParStream");
                }
            } catch (AvroAdaptorException e) {
                e.printStackTrace();
            }
        }
        dataFileReader.close();
        parstreamConn.commit();
        System.out.println("Committed to ParStream");

        parstreamConn.close();
        System.out.println("Closed connection to ParStream");
    }

    private static File getFileFromResources(String fileName) {
        try {
            InputStream inputStream = AvroAdaptorExample.class.getResourceAsStream(fileName);
            OutputStream outputStream = new FileOutputStream(fileName);

            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            inputStream.close();
            outputStream.flush();
            outputStream.close();
            return new File(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
