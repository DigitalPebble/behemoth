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

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.CliProcessor;

/**
 * Converts a Nutch segment into a Behemoth datastructure. The binary data can
 * be stored (or not), the text representation can be stored if available, the
 * parse or fetch metadata are stored in the document metadata.
 */
public class NutchSegmentConverterJob extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory
            .getLogger(NutchSegmentConverterJob.class);

    public static final String USAGE = "Parse a Behemoth corpus with GATE";
    
    public NutchSegmentConverterJob() {
        this(null);
    }
    
    public NutchSegmentConverterJob(Configuration conf) {
    	super(conf);
    }
    
    public void convert(Path nutchsegment, Path output) throws IOException {

    	Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJobName("Convert Nutch segment" + nutchsegment);
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(nutchsegment,
                Content.DIR_NAME));
        conf.set(Nutch.SEGMENT_NAME_KEY, nutchsegment.getName());
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(NutchSegmentConverterMapper.class);

        // no reducers
        job.setNumReduceTasks(0);

        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BehemothDocument.class);

        try {
        	job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Conversion: done");
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new NutchSegmentConverterJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
		CliProcessor cliProcessor = new CliProcessor(
				NutchSegmentConverterJob.class.getSimpleName(), USAGE);
		String inputOpt = cliProcessor.addRequiredOption("i", "input",
				"Input directory on local file system", true);
		String outputOpt = cliProcessor.addRequiredOption("o", "output",
				"Output directory on HDFS", true);

		try {
			cliProcessor.parse(args);
		} catch (ParseException me) {
			return -1;
		}

		Path segment = new Path(cliProcessor.getOptionValue(inputOpt));
		Path output = new Path(cliProcessor.getOptionValue(outputOpt));
		convert(segment, output);
		return 0;
    }

}
