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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

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
    String text = "<HTML><BODY>This is a <B>simple</B> test</HTML>";
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
    assertEquals("This is a simple test", output.getText());
  }

}
