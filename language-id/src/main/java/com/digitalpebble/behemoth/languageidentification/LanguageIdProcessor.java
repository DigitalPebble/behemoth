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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.cybozu.labs.langdetect.util.LangProfile;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;

public class LanguageIdProcessor implements DocumentProcessor {

	public static final Text languageMDKey = new Text("lang");

	private static final Logger LOG = LoggerFactory
			.getLogger(LanguageIdProcessor.class);

	private Configuration config;

	private final String[] defaultLanguagesToLoad = new String[] { "af", "ar",
			"bg", "bn", "cs", "da", "de", "el", "en", "es", "et", "fa", "fi",
			"fr", "gu", "he", "hi", "hr", "hu", "id", "it", "ja", "kn", "ko",
			"lt", "lv", "mk", "ml", "mr", "ne", "nl", "no", "pa", "pl", "pt",
			"ro", "ru", "sk", "sl", "so", "sq", "sv", "sw", "ta", "te", "th",
			"tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw" };

	public Configuration getConf() {
		return config;
	}

	public void setConf(Configuration conf) {
		config = conf;

		// TODO get list of languages to load from conf

		String[] languagesToLoad = defaultLanguagesToLoad;

		List<String> json_profiles = new ArrayList<String>();

		for (String langCode : languagesToLoad) {
			try {
				json_profiles.add(loadLanguageProfile(langCode));
			} catch (IOException e) {
				LOG.info("Can't load language profile for " + langCode);
			}
		}

		try {
			DetectorFactory.loadProfile(json_profiles);
		} catch (LangDetectException e) {
			LOG.info("Can't load language profiles");
		}
	}

	private static String loadLanguageProfile(String langCode)
			throws IOException {
		InputStream is = DetectorFactory.class.getClassLoader()
				.getResourceAsStream("profiles/" + langCode);
		String profile = IOUtils.toString(is);
		is.close();
		return profile;
	}

	public void close() {
	}

	public BehemothDocument[] process(BehemothDocument inputDoc,
			Reporter reporter) {
		// check that it has some text
		if (inputDoc.getText() == null) {
			LOG.info("No text for " + inputDoc.getUrl() + " skipping");
			reporter.getCounter("LANGUAGE ID", "MISSING TEXT").increment(1);
			return new BehemothDocument[] { inputDoc };
		}

		String lang = null;

		// skip docs with empty text
		if (inputDoc.getText().trim().isEmpty()) {
			LOG.info("Empty text for " + inputDoc.getUrl() + " skipping");
			reporter.getCounter("LANGUAGE ID", "EMPTY TEXT").increment(1);
			return new BehemothDocument[] { inputDoc };
		}

		try {
			Detector detector = DetectorFactory.create();
			detector.append(inputDoc.getText());
			lang = detector.detect();
			inputDoc.getMetadata(true).put(languageMDKey, new Text(lang));
		} catch (LangDetectException e) {
			LOG.error("Exception on doc " + inputDoc.getUrl(), e);
			lang = null;
		}

		if (reporter != null && lang != null)
			reporter.getCounter("LANGUAGE DETECTED", lang).increment(1);

		return new BehemothDocument[] { inputDoc };
	}

}
