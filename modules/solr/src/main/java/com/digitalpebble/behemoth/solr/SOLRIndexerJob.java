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

package com.digitalpebble.behemoth.solr;

import java.util.Random;

import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.CliProcessor;

/**
 * Sends annotated documents to SOLR for indexing
 */

public class SOLRIndexerJob extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(SOLRIndexerJob.class);

    public static final String USAGE = "Sends annotated documents to SOLR for indexing";
    
    public SOLRIndexerJob() {
    }

    public SOLRIndexerJob(Configuration conf) {
        super(conf);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new SOLRIndexerJob(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        final FileSystem fs = FileSystem.get(getConf());
        
		CliProcessor cliProcessor = new CliProcessor(
				SOLRIndexerJob.class.getSimpleName(),
				USAGE);
		String inputOpt = cliProcessor.addRequiredOption("i", "input",
				"Input directory on HDFS", true);
		String solrOpt = cliProcessor.addRequiredOption("l", "solr",
				"SOLR URL", true);

		try {
			cliProcessor.parse(args);
		} catch (MissingOptionException me) {
			return -1;
		}

        Path inputPath = new Path(cliProcessor.getOptionValue(inputOpt));
        String solrURL = cliProcessor.getOptionValue(solrOpt);

        Configuration conf = getConf();
        Job job = new Job(conf);

        job.setJarByClass(this.getClass());

        job.setJobName("Indexing " + inputPath + " into SOLR");

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SOLROutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BehemothDocument.class);

        job.setMapperClass(Mapper.class);
        // no reducer : send straight to SOLR at end of mapping
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, inputPath);
        final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-"
                + new Random().nextInt());
        FileOutputFormat.setOutputPath(job, tmp);

        conf.set("solr.server.url", solrURL);

        try {
        	job.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error(e);
        } finally {
            fs.delete(tmp, true);
            DistributedCache.purgeCache(conf);
        }

        return 0;
    }
}
