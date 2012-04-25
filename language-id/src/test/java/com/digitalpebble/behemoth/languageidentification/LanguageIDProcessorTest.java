package com.digitalpebble.behemoth.languageidentification;

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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.languageidentification.LanguageIdProcessor;

public class LanguageIDProcessorTest extends TestCase {

	LanguageIdProcessor langid = null;

	public void setUp() throws Exception {
		Configuration conf = BehemothConfiguration.create();
		langid = new LanguageIdProcessor();
		langid.setConf(conf);
	}

	public void tearDown() throws Exception {

	}

	public void testLanguageID() {
		String text = "Ceci est un texte en Francais, un peu court mais ca devrait etre assez pour la detection.";
		assertEquals("fr", testLanguage(text));
		text = "This text is in English, it is a bit short but it should be enough for detecting its language";
		assertEquals("en", testLanguage(text));
	}

	private String testLanguage(String text) {
		// Create a very simple Behemoth document
		String url = "dummyDoc.html";
		BehemothDocument doc = new BehemothDocument();
		doc.setContent(text.getBytes());
		doc.setText(text);
		doc.setUrl(url);
		// don't set the text as such
		// or the content type
		BehemothDocument[] outputs = langid.process(doc, null);
		// the output should contain only one document
		assertEquals(1, outputs.length);
		BehemothDocument output = outputs[0];
		// the output document should have a language metadata
		// and its value should be french
		Writable lang = output.getMetadata().get(
				LanguageIdProcessor.languageMDKey);
		return lang.toString();
	}

}
