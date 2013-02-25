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

package com.digitalpebble.behemoth.languageidentification;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentFilter;

public class LanguageIdMapper extends MapReduceBase implements
        Mapper<Text, BehemothDocument, Text, BehemothDocument> {

    private static final Logger LOG = LoggerFactory
            .getLogger(LanguageIdMapper.class);

    protected static LanguageIdProcessor processor;

    private DocumentFilter filter;

    public void map(Text text, BehemothDocument inputDoc,
            OutputCollector<Text, BehemothDocument> outputCollector,
            Reporter reporter) throws IOException {

        BehemothDocument[] documents = processor.process(inputDoc, reporter);
        if (documents != null) {
            for (int i = 0; i < documents.length; i++) {
                boolean keep = filter.keep(documents[i]);
                if (keep)
                    outputCollector.collect(text, documents[i]);
                else
                    reporter.incrCounter("LanguageIDMapper", "FILTERED", 1);
            }
        }
    }

    @Override
    public void configure(JobConf job) {
        filter = DocumentFilter.getFilters(job);
        if (processor == null) {
            long start = System.currentTimeMillis();
            processor = new LanguageIdProcessor();
            processor.setConf(job);
            long end = System.currentTimeMillis();
            LOG.info("LanguageIdProcessor initialised in " + (end - start)
                    + " msec");
        } else
            LOG.info("Reusing existing language processor");

    }
}
