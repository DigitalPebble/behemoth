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

package com.digitalpebble.behemoth.uima;

import java.net.URI;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.InputOutputCliProcessor;

public class UIMADriver extends Configured implements Tool {

	public final static String USAGE = "Process a Behemoth corpus with UIMA";
	
    private static final Logger LOG = LoggerFactory.getLogger(UIMADriver.class);

    public UIMADriver() {
        super(null);
    }

    public UIMADriver(Configuration conf) {
        super(conf);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new UIMADriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        final FileSystem fs = FileSystem.get(getConf());
        
		InputOutputCliProcessor cliProcessor = new InputOutputCliProcessor(
				UIMADriver.class.getSimpleName(),
				USAGE);
		String pearOpt = cliProcessor.addRequiredOption("p", "pear",
				"Path to PEAR file", true, "path_to_pear_file");

		try {
			cliProcessor.parse(args);
		} catch (ParseException me) {
			return -1;
		}

        Path inputPath = new Path(cliProcessor.getInputValue());
        Path outputPath = new Path(cliProcessor.getOutputValue());
        String pearPath = cliProcessor.getOptionValue(pearOpt);

        // check that the GATE application has been stored on HDFS
        Path zap = new Path(pearPath);
        if (fs.exists(zap) == false) {
            System.err.println("The UIMA application " + pearPath
                    + "can't be found on HDFS - aborting");
            return -1;
        }

        Configuration conf = getConf();
        Job job = new Job(getConf());
        job.setJarByClass(this.getClass());
        job.setJobName("Processing with UIMA application : " + pearPath);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BehemothDocument.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BehemothDocument.class);

        job.setMapperClass(UIMAMapper.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // push the UIMA pear onto the DistributedCache
        DistributedCache.addCacheFile(new URI(pearPath), conf);

        conf.set("uima.pear.path", pearPath);

        try {
        	job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            fs.delete(outputPath, true);
        } finally {
            DistributedCache.purgeCache(conf);
        }

        return 0;
    }

}
