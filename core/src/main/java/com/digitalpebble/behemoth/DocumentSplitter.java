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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.util.AnnotationsUtil;

/**
 * Splitter used by the BehemothReducer to generate multiple Behemoth documents
 * based on annotations. The newly generated documents will correspond to the
 * spans of the annotation in the original document. Anything outside these
 * spans will be ignored. TODO add a parameter for determining whether the
 * annotation is a boundary or a span
 **/
public class DocumentSplitter {

	private static final Logger LOG = LoggerFactory
			.getLogger(DocumentSplitter.class);

	// prefix used to define the annotation type and (optionally) feature name
	// to use for
	// splitting a document into subdocuments
	// if the value is empty - no feature constraint is done otherwise we expect
	// the value to be a regular expression
	// e.g. document.splitter.annotation => div.class=page
	public static final String docSplittingAnnotationParamName = "document.splitter.annotation";

	// true to copy the document features from the original document to the sub
	// documents, false otherwise. Defaults to true.
	public static final String transferDocFeaturesParamName = "document.splitter.transfer.features";

	private boolean splitterTransferFeatures = true;

	private String splittingannot;

	/**
	 * Checks whether any splitters have been specified in the configuration
	 **/
	public static boolean isRequired(JobConf conf) {
		DocumentSplitter splitter = DocumentSplitter.getSplitter(conf);
		return splitter.splittingannot.length() > 0;
	}

	// Builds a document filter given a conf object
	public static DocumentSplitter getSplitter(Configuration conf) {

		DocumentSplitter splitter = new DocumentSplitter();

		splitter.splitterTransferFeatures = conf.getBoolean(
				transferDocFeaturesParamName, true);

		// get the annotation to use for splitting
		splitter.splittingannot = conf.get(docSplittingAnnotationParamName, "");
		if (splitter.splittingannot.length() > 0)
			LOG.info("Splitting into subdocs with : " + splitter.splittingannot);
		else
			LOG.info("No splitting into subdocs");

		return splitter;
	}

	public BehemothDocument[] split(BehemothDocument input) {

		// need to sort the annotations
		AnnotationsUtil.sort(input.getAnnotations());

		// TODO if has only one span matching AND covers the whole text
		// return existing doc

		return new BehemothDocument[] { input };
	}

}
