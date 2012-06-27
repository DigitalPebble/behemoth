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

	private DocumentFilter docFilter;

	private DocumentSplitter docSplitter;

	/**
	 * Checks whether any filters or splitters have been specified in the
	 * configuration
	 **/
	public static boolean isRequired(JobConf conf) {
		if (DocumentSplitter.isRequired(conf)) return true;
		if (DocumentFilter.isRequired(conf)) return true;
		return false;
	}

	public void configure(JobConf conf) {
		this.docSplitter = DocumentSplitter.getSplitter(conf);
		this.docFilter = DocumentFilter.getFilters(conf);
	}

	public void close() throws IOException {
	}

	public void reduce(Text key, Iterator<BehemothDocument> doc,
			OutputCollector<Text, BehemothDocument> output, Reporter reporter)
			throws IOException {

		while (doc.hasNext()) {
			BehemothDocument inputDoc = doc.next();

			// TODO split THEN filter
			BehemothDocument[] subdocs = docSplitter.split(inputDoc);
			if (subdocs.length > 1) {
				reporter.incrCounter("BehemothReducer", "DOC SPLITTED", 1);
			}
			for (BehemothDocument bdoc : subdocs) {
				boolean keep = docFilter.keep(bdoc);
				if (!keep) {
					reporter.incrCounter("BehemothReducer",
							"DOC SKIPPED BY FILTERS", 1);
					continue;
				}
				output.collect(key, bdoc);
			}
		}

	}

}
