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

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.cli.InputOutputReplaceCliProcessor;

public class TikaDriver extends Configured implements Tool, TikaConstants {
	private transient static Logger log = LoggerFactory
			.getLogger(TikaDriver.class);
	
	public final static String USAGE = "Parse a Behemoth corpus with Tika";
	
	public TikaDriver() {
		super(null);
	}

	public TikaDriver(Configuration conf) {
		super(conf);
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(BehemothConfiguration.create(),
				new TikaDriver(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		final FileSystem fs = FileSystem.get(getConf());

		InputOutputReplaceCliProcessor cliProcessor = new InputOutputReplaceCliProcessor(
				TikaDriver.class.getSimpleName(), USAGE);
		String tikaOpt = cliProcessor
				.addOption(
						"t",
						"tikaProcessor",
						"The fully qualified name of a TikaProcessor class that handles the extraction",
						true);
		String mimeOpt = cliProcessor.addOption("m", "mimeType",
				"The mime type to use", true);

		try {
			cliProcessor.parse(args);
		} catch (ParseException me) {
			return -1;
		}

		Path inputPath = new Path(cliProcessor.getInputValue());
		Path outputPath = new Path(cliProcessor.getOutputValue());

		Job job = new Job(getConf());
		job.setJarByClass(this.getClass());

		String handlerName = cliProcessor.getOptionValue(tikaOpt);
		if (handlerName != null) {
			job.getConfiguration().set(TIKA_PROCESSOR_KEY, handlerName);
		}

		String mimeType = cliProcessor.getOptionValue(mimeOpt);
		if (mimeType != null) {
			job.getConfiguration().set(TikaConstants.TIKA_MIME_TYPE_KEY, mimeType);
		}

		job.setJobName("Processing with Tika");

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BehemothDocument.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BehemothDocument.class);

		job.setMapperClass(TikaMapper.class);

		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		try {
		        job.waitForCompletion(true);
			cliProcessor.replaceInputFile(getConf());
		} catch (Exception e) {
			e.printStackTrace();
			fs.delete(outputPath, true);
		} finally {
		}

		return 0;
	}
}
