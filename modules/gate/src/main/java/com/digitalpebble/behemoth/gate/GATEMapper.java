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

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Expects a GATE application packaged in a zip archive and stored on the
 * distributed cache
 **/

public class GATEMapper extends MapReduceBase implements
        Mapper<Text, BehemothDocument, Text, BehemothDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(GATEMapper.class);

    private Configuration config;

    private GATEProcessor processor;

    public void map(Text key, BehemothDocument behedoc,
            OutputCollector<Text, BehemothDocument> output, Reporter reporter)
            throws IOException {

        BehemothDocument[] outputDocs = processor.process(behedoc, reporter);
        for (BehemothDocument doc : outputDocs) {
            // TODO output under a different key?
            output.collect(key, doc);
        }
    }

    public void close() throws IOException {
        super.close();
        processor.close();
    }

    public void configure(JobConf job) {
        super.configure(job);
        config = job;

        // we try to load the gate application
        // using the gate.app file
        String application_path = job.get("gate.application.path");
        String gapp_file = job.get("gate.application.descriptor",
                "application.xgapp");

        URL applicationDescriptorURL = null;

        // the application will have been unzipped and put on the distributed
        // cache
        try {
            String applicationName = new File(application_path)
                    .getCanonicalFile().getName();
            // trim the zip
            if (applicationName.endsWith(".zip"))
                applicationName = applicationName.replaceAll(".zip", "");

            Path[] localArchives = DistributedCache.getLocalCacheArchives(job);
            // identify the right archive
            for (Path la : localArchives) {
                String localPath = la.toUri().toString();
                if (!localPath.endsWith(application_path))
                    continue;
                applicationDescriptorURL = new URL("file://" + localPath + "/"
                        + applicationName + "/" + gapp_file);
                break;
            }
        } catch (Exception e) {
            throw new RuntimeException(
                    "Impossible to retrieve gate application from distributed cache",
                    e);
        }

        if (applicationDescriptorURL == null)
            throw new RuntimeException("GATE app " + application_path
                    + "not available in distributed cache");

        processor = new GATEProcessor(applicationDescriptorURL);
        processor.setConf(config);
    }

}
