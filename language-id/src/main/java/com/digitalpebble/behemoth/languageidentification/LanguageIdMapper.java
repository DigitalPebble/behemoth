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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;

public class LanguageIdMapper extends MapReduceBase implements
		Mapper<Text, BehemothDocument, Text, BehemothDocument> {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(LanguageIdMapper.class);

	protected LanguageIdProcessor processor;

	public void map(Text text, BehemothDocument inputDoc,
			OutputCollector<Text, BehemothDocument> outputCollector,
			Reporter reporter) throws IOException {

		BehemothDocument[] documents = processor.process(inputDoc, reporter);
		if (documents != null) {
			for (int i = 0; i < documents.length; i++) {
				outputCollector.collect(text, documents[i]);
			}
		}
	}

	@Override
	public void configure(JobConf job) {
		if (processor == null) {
			processor = new LanguageIdProcessor();
		}
		processor.setConf(job);
	}
}
