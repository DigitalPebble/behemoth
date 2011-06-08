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

import org.apache.commons.cli.MissingOptionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.cli.CliProcessor;

/**
 * Utility class used to read the content of a Behemoth SequenceFile.
 **/
public class CorpusReader extends Configured implements Tool {

	public final static String USAGE = "Read a Behemoth Corpus on HDFS and output to System.out";
	
	public CorpusReader() {
	}
	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new CorpusReader(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        
		CliProcessor cliProcessor = new CliProcessor(CorpusReader.class.getSimpleName(), USAGE);
		String inputOpt = cliProcessor.addRequiredOption("i", "input",
				"Input directory on HDFS", true);
		String binaryOpt = cliProcessor.addOption("b",
				"showBinaryContent", "Show binary content", false);

		try {
			cliProcessor.parse(args);
		} catch (MissingOptionException me) {
			return -1;
		}

        Path input = new Path(cliProcessor.getOptionValue(inputOpt));

        boolean showBinaryContent = cliProcessor.hasOption(binaryOpt);

		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fss = fs.listStatus(input);
		for (FileStatus status : fss) {
			Path path = status.getPath();
			if (path.getName().startsWith("part-")
					|| path.getName().equals(input.getName())) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,
						conf);
				Text key = new Text();
				BehemothDocument value = new BehemothDocument();
				while (reader.next(key, value)) {
					System.out.println(value.toString(showBinaryContent));
				}
				reader.close();
			}
		}

        return 0;
    }

}
