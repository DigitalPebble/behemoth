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

package com.digitalpebble.behemoth.tika;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TikaProcessorTest extends TestCase {


  TikaProcessor tika = null;

  public void setUp() throws Exception {
    Configuration conf = BehemothConfiguration.create();
    tika = new TikaProcessor();
    tika.setConf(conf);
  }

  public void tearDown() throws Exception {

  }

  public void testTextExtractionTika() {
    // Create a very simple Behemoth document
    String text = "<HTML><head><TITLE>A TITLE</TITLE><meta name=\"keywords\" content=\"foo,bar\"/>" +
            "</head><BODY>This is a <B>simple</B> test</HTML>";
    String url = "dummyDoc.html";
    BehemothDocument doc = new BehemothDocument();
    doc.setContent(text.getBytes());
    doc.setUrl(url);
    // don't set the text as such
    // or the content type
    BehemothDocument[] outputs = tika.process(doc, null);
    // the output should contain only one document
    assertEquals(1, outputs.length);
    BehemothDocument output = outputs[0];
    // the output document must be marked as text/html
    assertEquals("text/html", output.getContentType());
    // and have the following text
    assertEquals("A TITLE\n\n\nThis is a simple test", output.getText().trim());
    Configuration conf = BehemothConfiguration.create();
    conf.setBoolean("tika.metadata", true);
    tika.setConf(conf);
    doc = new BehemothDocument();
    doc.setContent(text.getBytes());
    doc.setUrl(url);
    outputs = tika.process(doc, null);
    assertEquals(1, outputs.length);
    MapWritable metadata = outputs[0].getMetadata();
    Writable keywords = metadata.get(new Text("keywords"));
    assertNotNull("keywords", keywords);

    conf.setBoolean("tika.annotations", true);
    tika.setConf(conf);
    doc = new BehemothDocument();
    doc.setContent(text.getBytes());
    doc.setUrl(url);
    outputs = tika.process(doc, null);
    assertEquals(1, outputs.length);
    metadata = outputs[0].getMetadata();
    keywords = metadata.get(new Text("keywords"));
    assertNotNull("keywords", keywords);
    assertTrue(outputs[0].getAnnotations().size() > 0);
  }
}
