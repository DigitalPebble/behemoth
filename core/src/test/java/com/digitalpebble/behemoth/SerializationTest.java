/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.behemoth;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class SerializationTest extends TestCase {

    private Configuration conf;
    private FileSystem fs;
    private File file;

    @Override
    protected void setUp() throws Exception {
        conf = BehemothConfiguration.create();
        fs = FileSystem.getLocal(conf);
        file = new File("test_" + System.currentTimeMillis());
    }

    @Override
    protected void tearDown() throws Exception {
        fs.close();
    }

    public void testSerialization() throws IOException {
        BehemothDocument doc = new BehemothDocument();
        doc.setUrl("test");
        String tcontent = "This is home";
        doc.setContent(ByteBuffer.wrap(tcontent.getBytes()));
        doc.setText(tcontent);
        doc.setContentType("txt");
        Annotation annot = new Annotation();
        annot.setStart(0l);
        annot.setEnd(12l);
        annot.setType("annotType");

        BehemothDocumentUtil.getOrCreateAnnotations(doc).add(annot);

        DatumWriter<BehemothDocument> datumWriter = new SpecificDatumWriter<BehemothDocument>(
                BehemothDocument.class);
        DataFileWriter<BehemothDocument> dataFileWriter = new DataFileWriter<BehemothDocument>(
                datumWriter);
        dataFileWriter.create(doc.getSchema(), file);
        dataFileWriter.append(doc);
        dataFileWriter.close();

        DatumReader<BehemothDocument> userDatumReader = new SpecificDatumReader<BehemothDocument>(
                BehemothDocument.class);
        DataFileReader<BehemothDocument> dataFileReader = new DataFileReader<BehemothDocument>(
                file, userDatumReader);
        BehemothDocument doc2 = null;
        while (dataFileReader.hasNext()) {
            // Reuse user object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            doc2 = dataFileReader.next(doc2);
        }

        file.delete();

        // TODO check the values
    }

}
