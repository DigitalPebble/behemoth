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

package com.digitalpebble.behemoth.io.nutch;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Content;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Converts a Nutch segment into a Behemoth datastructure. The binary data can
 * be stored (or not), the text representation can be stored if available, the
 * parse or fetch metadata are stored in the document metadata.
 */
public class NutchSegmentConverterJob extends Configured implements Tool,
        Mapper<Text, Content, Text, BehemothDocument> {

    public static final Logger LOG = LoggerFactory
            .getLogger(NutchSegmentConverterJob.class);

    public NutchSegmentConverterJob() {
        this(null);
    }

    public NutchSegmentConverterJob(Configuration conf) {
        super(conf);
    }

    public void configure(JobConf job) {
        setConf(job);
    }

    public void close() {
    }

    private Text newKey = new Text();

    public void map(Text key, Content content,
            OutputCollector<Text, BehemothDocument> output, Reporter reporter)
            throws IOException {

        BehemothDocument behemothDocument = new BehemothDocument();

        int status = Integer.parseInt(content.getMetadata().get(
                Nutch.FETCH_STATUS_KEY));
        if (status != CrawlDatum.STATUS_FETCH_SUCCESS) {
            // content not fetched successfully, skip document
            LOG.debug("Skipping " + key
                    + " as content is not fetched successfully");
            return;
        }

        // TODO store the fetch metadata in the Behemoth document
        // store the binary content and mimetype in the Behemoth document

        String contentType = content.getContentType();
        byte[] binarycontent = content.getContent();
        behemothDocument.setUrl(key.toString());
        behemothDocument.setContent(binarycontent);
        behemothDocument.setContentType(contentType);
        output.collect(key, behemothDocument);
    }

    public void convert(Path nutchsegment, Path output) throws IOException {

        JobConf job = new JobConf(getConf());
        job.setJobName("Convert Nutch segment" + nutchsegment);
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(nutchsegment,
                Content.DIR_NAME));
        job.set(Nutch.SEGMENT_NAME_KEY, nutchsegment.getName());
        job.setInputFormat(SequenceFileInputFormat.class);
        job.setMapperClass(NutchSegmentConverterJob.class);

        // no reducers
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BehemothDocument.class);

      long start = System.currentTimeMillis();
      JobClient.runJob(job);
      long finish = System.currentTimeMillis();
      if (LOG.isInfoEnabled()) {
        LOG.info("NutchSegmentConverterJob completed.  Timing: " + (finish - start) + " ms");
      }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new NutchSegmentConverterJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        String usage = "Usage: SegmentConverter segment output";

        if (args.length == 0) {
            System.err.println(usage);
            System.exit(-1);
        }
        Path segment = new Path(args[0]);
        Path output = new Path(args[1]);
        convert(segment, output);
        return 0;
    }

}
