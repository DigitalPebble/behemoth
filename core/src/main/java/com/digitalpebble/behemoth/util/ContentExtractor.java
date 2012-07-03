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
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Stores the content from Behemoth documents into a local directory
 **/
public class ContentExtractor extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory
			.getLogger(ContentExtractor.class);

	public ContentExtractor() {
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(BehemothConfiguration.create(),
				new ContentExtractor(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Options options = new Options();
		// automatically generate the help statement
		HelpFormatter formatter = new HelpFormatter();
		// create the parser
		CommandLineParser parser = new GnuParser();

		options.addOption("h", "help", false, "print this message");
		options.addOption("i", "input", true, "Behemoth corpus");
		options.addOption("o", "output", true, "local corpus dir");

		// parse the command line arguments
		try {
			CommandLine line = parser.parse(options, args);
			String input = line.getOptionValue("i");
			String output = line.getOptionValue("o");
			if (line.hasOption("help")) {
				formatter.printHelp("ContentExtractor", options);
				return 0;
			}
			if (input == null || output == null) {
				formatter.printHelp("ContentExtractor", options);
				return -1;
			}
			generateDocs(input, output);

		} catch (ParseException e) {
			formatter.printHelp("ContentExtractor", options);
		}
		return 0;
	}

	private void generateDocs(String inputf, String outputf) throws IOException {
		Path input = new Path(inputf);

		File output = new File(outputf);
		if (output.exists() && output.isFile()) {
			System.err.println("Output " + outputf + " already exists");
			return;
		}
		if (output.exists() == false)
			output.mkdirs();

		FileSystem fs = input.getFileSystem(getConf());
		FileStatus[] statuses = fs.listStatus(input);
		int count[] = { 0 };
		for (int i = 0; i < statuses.length; i++) {
			FileStatus status = statuses[i];
			Path suPath = status.getPath();
			if (suPath.getName().equals("_SUCCESS"))
				continue;
			generateDocs(suPath, output, count);
		}
	}

	private void generateDocs(Path input, File dir, int[] count)
			throws IOException {

		Reader[] cacheReaders = SequenceFileOutputFormat.getReaders(getConf(),
				input);
		for (Reader current : cacheReaders) {
			// read the key + values in that file
			Text key = new Text();
			BehemothDocument inputDoc = new BehemothDocument();
			FileOutputStream writer = null;
			while (current.next(key, inputDoc)) {
				count[0]++;
				if (inputDoc.getContent() == null)
					continue;
				try {
					File outputFile = new File(dir, Integer.toString(count[0]));
					if (outputFile.exists() == false)
						outputFile.createNewFile();

					writer = new FileOutputStream(outputFile);
					writer.write(inputDoc.getContent());

				} catch (Exception e) {
					LOG.error(
							"Exception on doc [" + count[0] + "] "
									+ key.toString(), e);
				} finally {
					if (writer != null)
						writer.close();
				}
			}
			current.close();
		}
	}

}
