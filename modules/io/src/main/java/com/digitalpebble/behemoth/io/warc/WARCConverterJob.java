package com.digitalpebble.behemoth.io.warc;

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

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.cli.InputOutputCliProcessor;

/**
 * Converts a WARC archive into a Behemoth datastructure for further processing
 */
public class WARCConverterJob extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory
            .getLogger(WARCConverterJob.class);

    public final static String USAGE = "Convert a WARC Web Archive into a Behemoth Corpus";
    
    public WARCConverterJob() {
        this(null);
    }

    public WARCConverterJob(Configuration conf) {
        super(conf);
    }

    public void configure(JobConf job) {
        setConf(job);
    }

    public void close() {
    }

    public void convert(Path warcpath, Path output) throws IOException {

        JobConf job = new JobConf(getConf());
        job.setJobName("Convert WARC " + warcpath);

        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, warcpath);
        job.setInputFormat(WarcFileInputFormat.class);

        job.setMapperClass(WarcConverterMapper.class);

        // no reducers
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BehemothDocument.class);

        JobClient.runJob(job);
        if (LOG.isInfoEnabled()) {
            LOG.info("Conversion: done");
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new WARCConverterJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        InputOutputCliProcessor cliProcessor = new InputOutputCliProcessor(
                WARCConverterJob.class.getSimpleName(), USAGE);
        try {
            cliProcessor.parse(args);
        } catch (ParseException me) {
            return -1;
        }
        Path segment = new Path(cliProcessor.getInputValue());
        Path output = new Path(cliProcessor.getOutputValue());
        convert(segment, output);
        return 0;
    }

}
