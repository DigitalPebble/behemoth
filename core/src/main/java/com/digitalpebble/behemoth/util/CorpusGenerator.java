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

import com.digitalpebble.behemoth.BehemothDocument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Generates a SequenceFile containing BehemothDocuments given a local
 * directory. The BehemothDocument gets its byte content and URL. The detection
 * of MIME-type and text extraction can be done later using the TikaProcessor.
 */

public class CorpusGenerator {
  private transient static Logger log = LoggerFactory.getLogger(CorpusGenerator.class);
  private Path input, output;
  private Reporter reporter;

  public enum Counters{DOC_COUNT};

  public CorpusGenerator(Path input, Path output) {
    this.input = input;
    this.output = output;
  }

  public CorpusGenerator(Path input, Path output, Reporter reporter){
    this.input = input;
    this.output = output;
    this.reporter = reporter;
  }

  public long generate(boolean recurse) throws IOException {
    long result = 0;
    // read from input path
    // create new Content object and add it to the SequenceFile
    Text key = new Text();
    BehemothDocument value = new BehemothDocument();
    SequenceFile.Writer writer = null;
    try {
      Configuration conf = new Configuration();
      FileSystem fs = output.getFileSystem(conf);
      writer = SequenceFile.createWriter(fs, conf, output,
              key.getClass(), value.getClass());
      PerformanceFileFilter pff = new PerformanceFileFilter(writer, key,
              value, conf, reporter);
      // iterate on the files in the source dir
      result = processFiles(conf, input, recurse, pff);

    } finally {
      IOUtils.closeStream(writer);
    }
    return result;
  }

  public static void main(String argv[]) throws Exception {

    // Populate a SequenceFile with the content of a local directory

    String usage = "Content localdir outputDFSDir [--recurse]";

    if (argv.length < 2) {
      System.out.println("usage:" + usage);
      return;
    }


    Path inputDir = new Path(argv[0]);

    Path output = new Path(argv[1]);

    boolean recurse = false;
    if (argv.length > 2 && argv[2].equals("--recurse")) {
      recurse = true;
    }

    CorpusGenerator generator = new CorpusGenerator(inputDir, output);
    long count = generator.generate(recurse);//TODO: add support for how many docs were converted
    System.out.println(count + " docs converted");
  }

  private static long processFiles(Configuration conf, Path input, boolean recurse,
                                   PerformanceFileFilter pff) throws IOException {
    FileSystem fs = input.getFileSystem(conf);
    FileStatus[] statuses = fs.listStatus(input, pff);
    for (int i = 0; i < statuses.length; i++) {
      FileStatus status = statuses[i];
      if (recurse == true) {
        processFiles(conf, status.getPath(), recurse, pff);
      }
    }
    return pff.counter;
  }

  // Java hack to move the work of processing files into a filter, so that we
  // can process large directories of files
  // without having to create a huge list of files
  static class PerformanceFileFilter implements PathFilter {
    long counter = 0;
    PathFilter defaultIgnores = new PathFilter() {

      public boolean accept(Path file) {
        String name = file.getName();
        return name.startsWith(".") == false;// ignore hidden
        // directories
      }
    };


    private SequenceFile.Writer writer;
    private Text key;
    private BehemothDocument value;
    private Configuration conf;
    private Reporter reporter;

    public PerformanceFileFilter(SequenceFile.Writer writer, Text key,
                                 BehemothDocument value, Configuration conf, Reporter reporter) {
      this.writer = writer;
      this.key = key;
      this.value = value;
      this.conf = conf;
      this.reporter = reporter;
    }


    public boolean accept(Path file) {
      try {
        FileSystem fs = file.getFileSystem(conf);
        if (defaultIgnores.accept(file) && fs.getFileStatus(file).isDir() == false) {
          String URI = file.toUri().toString();
          //Hmm, kind of dangerous to do this
          byte[] fileBArray = new byte[(int) fs.getFileStatus(file).getLen()];
          FSDataInputStream fis = null;
          try {
            //TODO: better handling of files here, as reading in a sequence file would be really
            fis = fs.open(file);
            fis.readFully(0, fileBArray);
            fis.close();
            key.set(URI);
            // fill the values for the content object
            value.setUrl(URI);
            value.setContent(fileBArray);
            //TODO: is there a way to stream this in?
            writer.append(key, value);
            counter++;
            if (reporter != null){
              reporter.incrCounter(Counters.DOC_COUNT, 1);
            }
          } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        // if it is a directory, accept it so we can possibly recurse on it,
        // otherwise we don't care about actually accepting the file, since
        // all the work is done in the accept method here.
        return fs.getFileStatus(file).isDir();
      } catch (IOException e) {
        log.error("Exception", e);
      }
      return false;
    }
  }

}
