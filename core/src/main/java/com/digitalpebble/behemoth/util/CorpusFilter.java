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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.BehemothMapper;

/**
 * Utility class used to filter the content of a Behemoth SequenceFile.
 * 
 * @see DocumentFilter
 **/
public class CorpusFilter extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(BehemothConfiguration.create(),
				new CorpusFilter(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Options options = new Options();
		// automatically generate the help statement
		HelpFormatter formatter = new HelpFormatter();
		// create the parser
		CommandLineParser parser = new GnuParser();

		options.addOption("h", "help", false, "print this message");
		options.addOption("i", "input", true, "input Behemoth corpus");
		options.addOption("o", "output", true, "output Behemoth corpus");

		// parse the command line arguments
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
			String input = line.getOptionValue("i");
			String output = line.getOptionValue("o");
			if (line.hasOption("help")) {
				formatter.printHelp("CorpusFilter", options);
				return 0;
			}
			if (input == null | output == null) {
				formatter.printHelp("CorpusFilter", options);
				return -1;
			}
		} catch (ParseException e) {
			formatter.printHelp("CorpusFilter", options);
		}

		final FileSystem fs = FileSystem.get(getConf());

		Path inputPath = new Path(line.getOptionValue("i"));
		Path outputPath = new Path(line.getOptionValue("o"));

		JobConf job = new JobConf(getConf());
		job.setJarByClass(this.getClass());

		job.setJobName("CorpusFilter : " + inputPath.toString());

		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BehemothDocument.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BehemothDocument.class);

		boolean isFilterRequired = BehemothMapper.isRequired(job);
		// should be the case here
		if (!isFilterRequired) {
			System.err
					.println("No filters configured. Check your behemoth-site.xml");
			return -1;
		}
		job.setMapperClass(BehemothMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		try {
			JobClient.runJob(job);
		} catch (Exception e) {
			e.printStackTrace();
			fs.delete(outputPath, true);
		} finally {
		}

		return 0;
	}
}
