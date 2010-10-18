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

package com.digitalpebble.behemoth.util;

import java.io.File;
import java.io.FileInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Generates a SequenceFile containing BehemothDocuments given a local
 * directory. The BehemothDocument gets its byte content and URL. The detection of
 * MIME-type and text extraction can be done later using the TikaProcessor.
 **/

public class CorpusGenerator {

  public static void main(String argv[]) throws Exception {

    // Populate a SequenceFile with the content of a local directory

    String usage = "Content localdir outputDFSDir";

    if (argv.length < 2) {
      System.out.println("usage:" + usage);
      return;
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    File inputDir = new File(argv[0]);

    Path output = new Path(argv[1]);

    // read from input path
    // create new Content object and add it to the SequenceFile
    Text key = new Text();
    BehemothDocument value = new BehemothDocument();
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(fs, conf, output, key.getClass(),
          value.getClass());

      // iterate on the files in the source dir
      for (File file : inputDir.listFiles()) {

        if (file.isDirectory())
          continue;

        String URI = file.toURI().toString();

        byte[] fileBArray = new byte[(int) file.length()];
        FileInputStream fis = new FileInputStream(file);
        fis.read(fileBArray);
        fis.close();

        key.set(URI);

        // fill the values for the content object
        value.setUrl(URI);
        value.setContent(fileBArray);

        writer.append(key, value);
      }

    } finally {
      IOUtils.closeStream(writer);
    }

  }

}
