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

public class UIMADriver extends Configured implements Tool {

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

        if (args.length != 3) {
            String syntax = "com.digitalpebble.behemoth.uima.UIMADriver in out path_pear_file";
            System.err.println(syntax);
            return -1;
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        String pearPath = args[2];

        // check that the GATE application has been stored on HDFS
        Path zap = new Path(pearPath);
        if (fs.exists(zap) == false) {
            System.err.println("The UIMA application " + pearPath
                    + "can't be found on HDFS - aborting");
            return -1;
        }

        JobConf job = new JobConf(getConf());
        job.setJarByClass(this.getClass());
        job.setJobName("Processing with UIMA application : " + pearPath);

        job.setInputFormat(SequenceFileInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BehemothDocument.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BehemothDocument.class);

        job.setMapperClass(UIMAMapper.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // push the UIMA pear onto the DistributedCache
        DistributedCache.addCacheFile(new URI(pearPath), job);

        job.set("uima.pear.path", pearPath);

        try {
          long start = System.currentTimeMillis();
          JobClient.runJob(job);
          long finish = System.currentTimeMillis();
          if (LOG.isInfoEnabled()) {
            LOG.info("UIMADriver completed. Timing: " + (finish - start) + " ms");
          }
        } catch (Exception e) {
            e.printStackTrace();
            fs.delete(outputPath, true);
        } finally {}

        return 0;
    }

}
