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

package com.digitalpebble.behemoth.gate;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.hadoop.conf.Configuration;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

import junit.framework.TestCase;

public class GATEProcessorTest extends TestCase {

  private GATEProcessor gate = null;

  public void setUp() throws Exception {
    // load the resources from the test environment
    String testDataDir = System.getProperty("test.build.data");
    File ANNIEApp = new File(testDataDir, "ANNIE.zip");
    File unzippedANNIE = unzip(ANNIEApp);    
    File appDescriptor = new File(unzippedANNIE, "application.xgapp");
    // try loading the GATE Processor
    Configuration conf = BehemothConfiguration.create();
    conf.set("gate.application.path", unzippedANNIE.getCanonicalPath());
    conf.set("gate.annotationset.output", "");
    conf.set("gate.annotations.filter", "Token");
    gate = new GATEProcessor(appDescriptor.toURI().toURL());
    gate.setConf(conf);
  }

  public void tearDown() throws Exception {
    // close the GATE Processor
    gate.close();
  }

  public void testTokenizationANNIE() {
    // Create a very simple Behemoth document
    String text = "This is a simple test";
    String url = "dummyURL";
    BehemothDocument doc = new BehemothDocument();
    doc.setContent(text.getBytes());
    doc.setUrl(url);
    doc.setContentType("text/plain");
    // don't set the text as such
    // or any metadata at all
    BehemothDocument[] outputs = gate.process(doc, null);
    // the output should contain only one document
    assertEquals(1, outputs.length);
    BehemothDocument output = outputs[0];
    // the output document must have 5 annotations of type token
    // see gate.annotations.filter in Configuration above
    assertEquals(5, output.getAnnotations().size());
  }

  static final int BUFFER = 2048;

  /**
   * Unzips the argument into the temp directory and returns the unzipped
   * location. The zip must have a root dir element and not just a flat list of
   * files
   **/
  public static File unzip(File inputZip) {

    File rootDir = null;
    try {
      BufferedOutputStream dest = null;
      BufferedInputStream is = null;
      ZipEntry entry;
      ZipFile zipfile = new ZipFile(inputZip);
      File test = File.createTempFile("aaa", "aaa");
      String tempDir = test.getParent();
      test.delete();
      Enumeration e = zipfile.entries();
      while (e.hasMoreElements()) {
        entry = (ZipEntry) e.nextElement();
        is = new BufferedInputStream(zipfile.getInputStream(entry));
        int count;
        byte data[] = new byte[BUFFER];
        File target = new File(tempDir, entry.getName());
        if (entry.getName().endsWith("/")) {
          target.mkdir();
          if (rootDir == null) rootDir = target;
          continue;
        }
        FileOutputStream fos = new FileOutputStream(target);
        dest = new BufferedOutputStream(fos, BUFFER);
        while ((count = is.read(data, 0, BUFFER)) != -1) {
          dest.write(data, 0, count);
        }
        dest.flush();
        dest.close();
        is.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return rootDir;
  }

}
