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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;

public class LanguageIdProcessor implements DocumentProcessor {

	public static final Text languageMDKey = new Text("lang");
	
	private static final Logger LOG = LoggerFactory
			.getLogger(LanguageIdProcessor.class);

	private Configuration config;

	public Configuration getConf() {
		return config;
	}

	public void setConf(Configuration conf) {
		config = conf;
	}

	public void close() {
	}

	public BehemothDocument[] process(BehemothDocument inputDoc,
			Reporter reporter) {
		// check that it has some text
		if (inputDoc.getText() == null) {
			LOG.info("No text for " + inputDoc.getUrl()
					+ " skipping");
			return new BehemothDocument[] { inputDoc };
		}

		String lang = null;
		
		try {
			Detector detector = DetectorFactory.create();
			detector.append(inputDoc.getText());
			lang = detector.detect();
			inputDoc.getMetadata().put(languageMDKey, new Text(lang));
		} catch (LangDetectException e) {
			e.printStackTrace();
			lang = null;
		}

		if (reporter != null && lang != null)
			reporter.getCounter("LANGUAGE DETECTED", lang).increment(1);

		return new BehemothDocument[] { inputDoc };
	}

}
