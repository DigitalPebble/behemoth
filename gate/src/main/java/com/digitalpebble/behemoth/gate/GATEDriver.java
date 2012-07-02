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

package com.digitalpebble.behemoth.gate;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
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
import com.digitalpebble.behemoth.BehemothReducer;

public class GATEDriver extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(GATEDriver.class);

    public GATEDriver() {
        super(null);
    }

    public GATEDriver(Configuration conf) {
        super(conf);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new GATEDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        final FileSystem fs = FileSystem.get(getConf());

        if (args.length < 3 | args.length > 4) {
            String syntax = "com.digitalpebble.behemoth.gate.GATEDriver in out path_gate_file [-XML]";
            System.err.println(syntax);
            return -1;
        }

        boolean dumpGATEXML = false;

        for (String arg : args) {
            if (arg.equalsIgnoreCase("-xml"))
                dumpGATEXML = true;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String zip_application_path = args[2];

        // check that the GATE application has been stored on HDFS
        Path zap = new Path(zip_application_path);
        if (fs.exists(zap) == false) {
            System.err.println("The GATE application " + zip_application_path
                    + "can't be found on HDFS - aborting");
            return -1;
        }

        JobConf job = new JobConf(getConf());
        // MUST not forget the line below
        job.setJarByClass(this.getClass());

        job.setJobName("Processing " + args[0] + " with GATE application from "
                + zip_application_path);

        job.setInputFormat(SequenceFileInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        job.setOutputKeyClass(Text.class);

        if (dumpGATEXML) {
            job.setOutputValueClass(Text.class);
            job.setMapperClass(GATEXMLMapper.class);
        } else {
            job.setOutputValueClass(BehemothDocument.class);
            job.setMapperClass(GATEMapper.class);
        }

        // detect if any filters or splitters have been defined
        // and activate the reducer accordingly
        boolean isFilterRequired = BehemothReducer.isRequired(job);
        if (isFilterRequired)
            job.setReducerClass(BehemothReducer.class);
        else {
            job.setNumReduceTasks(0);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // push the zipped_gate_application onto the DistributedCache
        DistributedCache.addCacheArchive(new URI(zip_application_path), job);

        job.set("gate.application.path", zip_application_path.toString());

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
