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
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Reducer which can filter documents before they are written out. TODO
 * split documents based on annotations
 ***/

public class BehemothReducer implements
		Reducer<Text, BehemothDocument, Text, BehemothDocument> {

	public static final Logger LOG = LoggerFactory
			.getLogger(BehemothReducer.class);

	// prefix used to define the annotation types and (optionally) feature names
	// to use for
	// splitting a document into subdocuments
	// if the value is empty - no feature constraint is done otherwise we expect
	// the value to be a regular expression
	// e.g. behemothreducer.splitter.annotation.div => class=page
	public static final String docSplittingAnnotationParamName = "behemothreducer.splitter.annotation";

	// true to copy the document features from the original document to the sub
	// documents, false otherwise
	public static final String transferDocFeaturesParamName = "behemothreducer.splitter.transfer.features";

	private boolean splitterTransferFeatures = true;

	private DocumentFilter docFilter;

	public void configure(JobConf conf) {
		splitterTransferFeatures = conf.getBoolean(
				transferDocFeaturesParamName, true);
		Map<String, String> annotSplitMap = conf
				.getValByRegex(docSplittingAnnotationParamName + ".+");

		this.docFilter = DocumentFilter.getFilters(conf);
	}

	public void close() throws IOException {
	}

	public void reduce(Text key, Iterator<BehemothDocument> doc,
			OutputCollector<Text, BehemothDocument> output, Reporter reporter)
			throws IOException {

		while (doc.hasNext()) {
			BehemothDocument inputDoc = doc.next();
			boolean keep = docFilter.keep(inputDoc);
			if (!keep) {
				reporter.incrCounter("BehemothReducer",
						"DOC SKIPPED BY FILTERS", 1);
				continue;
			}
			output.collect(key, inputDoc);
		}

	}

}
