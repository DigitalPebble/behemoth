package com.digitalpebble.behemoth.io.sequencefile;
/*
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


import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 **/
public class SequenceFileConverterJob extends AbstractJob {
  private transient static Logger log = LoggerFactory.getLogger(SequenceFileConverterJob.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(BehemothConfiguration.create(),
            new SequenceFileConverterJob(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    int result = 0;
    addInputOption();
    addOutputOption();
    if (parseArguments(args) == null) {
      return -1;
    }
    Path input = getInputPath();
    Path output = getOutputPath();

    Job job = prepareJob(input, output, SequenceFileInputFormat.class, SequenceFileConverterMapper.class,
            Text.class, BehemothDocument.class,
            SequenceFileOutputFormat.class);
    job.setJobName("Convert Sequence File: " + input);
    job.waitForCompletion(true);

    if (log.isInfoEnabled()) {
      log.info("Conversion: done");
    }
    return result;
  }


}
