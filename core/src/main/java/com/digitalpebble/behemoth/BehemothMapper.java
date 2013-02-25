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

package com.digitalpebble.behemoth;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Mapper which can filter documents before they are written out.
 ***/

public class BehemothMapper implements
        Mapper<Text, BehemothDocument, Text, BehemothDocument> {

    public static final Logger LOG = LoggerFactory
            .getLogger(BehemothMapper.class);

    private DocumentFilter docFilter;

    /**
     * Checks whether any filters have been specified in the configuration
     **/
    public static boolean isRequired(JobConf conf) {
        return (DocumentFilter.isRequired(conf));
    }

    public void configure(JobConf conf) {
        this.docFilter = DocumentFilter.getFilters(conf);
    }

    public void close() throws IOException {
    }

    public void map(Text key, BehemothDocument inputDoc,
            OutputCollector<Text, BehemothDocument> output, Reporter reporter)
            throws IOException {
        boolean keep = docFilter.keep(inputDoc);
        if (!keep) {
            reporter.incrCounter("BehemothMapper", "DOC SKIPPED BY FILTERS", 1);
            return;
        }
        output.collect(key, inputDoc);
    }

}
