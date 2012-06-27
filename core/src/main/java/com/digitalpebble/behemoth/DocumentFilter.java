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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Filter;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters used by Mappers / Reducers to skip Behemoth documents based on their
 * metadata. Can have either positive or negative filters but not both. The
 * filters values are regular expressions and the document will be kept or
 * skipped if it matches ANY of the filters. It can filter based on the document
 * URL and or mime-type using regular expressions.
 **/
public class DocumentFilter {

	private static final Logger LOG = LoggerFactory
			.getLogger(DocumentFilter.class);

	public static final String DocumentFilterParamNamePrefixKeep = "document.filter.md.keep.";
	public static final String DocumentFilterParamNamePrefixSkip = "document.filter.md.skip.";
	public static final String DocumentFilterParamNameURLFilterKeep = "document.filter.url.keep.";
	public static final String DocumentFilterParamNameMimeTypeFilterKeep = "document.filter.mimetype.keep.";

	private Map<String, String> KVpatterns = new HashMap<String, String>();

	private boolean negativeMode = true;

	private Pattern URLRegex;
	
	private Pattern MimetypeRegex;

	/**
	 * Checks whether any splitters have been specified in the configuration
	 **/
	public static boolean isRequired(Configuration conf) {
		DocumentFilter fitler = DocumentFilter.getFilters(conf);
		if (fitler.KVpatterns.size() > 0)
			return true;
		if (fitler.URLRegex != null)
			return true;
		if (fitler.MimetypeRegex != null)
			return true;
		return false;
	}

	// Builds a document filter given a conf object
	public static DocumentFilter getFilters(Configuration conf) {
		// extracts the patterns
		Map<String, String> PositiveKVpatterns = conf
				.getValByRegex(DocumentFilterParamNamePrefixKeep + ".+");
		Map<String, String> NegativeKVpatterns = conf
				.getValByRegex(DocumentFilterParamNamePrefixSkip + ".+");

		Map<String, String> tmpMap;

		DocumentFilter filter = new DocumentFilter();

		// has to be either prositive or negative but not both
		if (PositiveKVpatterns.size() > 0 && NegativeKVpatterns.size() > 0) {
			throw new RuntimeException(
					"Can't have positive AND negative document filters - check your configuration");
		} else if (PositiveKVpatterns.size() > 0) {
			filter.negativeMode = false;
			tmpMap = PositiveKVpatterns;
		} else {
			filter.negativeMode = true;
			tmpMap = NegativeKVpatterns;
		}

		// normalise the keys
		Iterator<Entry<String, String>> kviter = tmpMap.entrySet().iterator();
		while (kviter.hasNext()) {
			Entry<String, String> ent = kviter.next();
			String k = ent.getKey();
			String v = ent.getValue();
			k = k.substring(DocumentFilterParamNamePrefixKeep.length());

			StringBuffer message = new StringBuffer();
			if (filter.negativeMode)
				message.append("Negative ");
			else
				message.append("Positive ");
			message.append("filter found : ").append(k).append(" = ").append(v);
			LOG.info(message.toString());

			filter.KVpatterns.put(k, v);
		}

		String URLPatternS = conf.get(DocumentFilterParamNameURLFilterKeep, "");
		if (URLPatternS.length() > 0) {
			try {
				filter.URLRegex = Pattern.compile(URLPatternS);
			} catch (PatternSyntaxException e) {
				filter.URLRegex = null;
				LOG.error("Can't create regular expression for URL from " + URLPatternS);
			}
		}
		
		String MTPatternS = conf.get(DocumentFilterParamNameMimeTypeFilterKeep, "");
		if (MTPatternS.length() > 0) {
			try {
				filter.MimetypeRegex = Pattern.compile(MTPatternS);
			} catch (PatternSyntaxException e) {
				filter.MimetypeRegex = null;
				LOG.error("Can't create regular expression for MimeType from " + MTPatternS);
			}
		}

		return filter;
	}

	/** Returns true if the document can be kept, false otherwise **/
	public boolean keep(BehemothDocument input) {
		// filter if null
		if (input == null)
			return false;
		
		// check on the URL
		if (URLRegex!=null){
			if (input.getUrl()==null) return false;
			boolean match = URLRegex.matcher(input.getUrl()).matches();
			if (!match) return false;
		}
		
		// check on the MimeType
		if (MimetypeRegex!=null){
			if (input.getContentType()==null) return false;
			boolean match = MimetypeRegex.matcher(input.getContentType()).matches();
			if (!match) return false;
		}

		MapWritable metadata = input.getMetadata();
		// no rules at all -> fine!
		if (KVpatterns.size() == 0)
			return true;

		// document MUST have a certain value to be kept
		if (metadata == null || metadata.isEmpty()) {
			if (!negativeMode)
				return false;
			else
				return true;
		}

		boolean hasMatch = false;

		// find common keys between filters and content of doc
		for (Writable wkey : metadata.keySet()) {
			String key = wkey.toString();
			String value = metadata.get(wkey).toString();
			// see if it matches
			String regex = KVpatterns.get(key);
			if (regex == null)
				continue;
			if (value.matches(regex)) {
				hasMatch = true;
				break;
			}
		}

		if (hasMatch) {
			return (!negativeMode);
		}

		// no negative rule matching
		if (negativeMode)
			return true;

		// no positive rule matching
		return false;
	}

}
