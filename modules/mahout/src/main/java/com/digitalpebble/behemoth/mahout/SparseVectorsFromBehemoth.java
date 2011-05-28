package com.digitalpebble.behemoth.mahout;

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

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.InputOutputCliProcessor;
import com.digitalpebble.behemoth.ParseArgumentException;

/**
 * Similar to SparseVectorsFromSequenceFiles but gets the Tokens from a Behemoth
 * corpus Converts a given set of sequence files into SparseVectors
 */
public final class SparseVectorsFromBehemoth {

	private static final Logger log = LoggerFactory
			.getLogger(SparseVectorsFromBehemoth.class);

	private SparseVectorsFromBehemoth() {
	}

	public static void main(String[] args) throws Exception {
		InputOutputCliProcessor cliProcessor = new InputOutputCliProcessor(
				SparseVectorsFromBehemoth.class,
				"Similar to SparseVectorsFromSequenceFiles but gets the Tokens from a Behemoth corpus. "
						+ "Converts a given set of sequence files into SparseVectors");

		String typeNameOpt = cliProcessor.addRequiredOption("t", "typeToken",
				"The annotation type for Tokens", true);

		String featureNameOpt = cliProcessor.addRequiredOption("f",
				"featureName",
				"The name of the feature containing the token values", true);

		String minSupportOpt = cliProcessor.addOption("s", "minSupport",
				"(Optional) Minimum Support. Default Value: 2", true);

		String chunkSizeOpt = cliProcessor.addOption("chunk", "chunkSize",
				"(Optional) The chunkSize in MegaBytes. 100-10000 MB", true);

		String weightOpt = cliProcessor.addOption("wt", "weight",
				"(Optional) The kind of weight to use. Currently TF or TFIDF",
				true);

		String minDFOpt = cliProcessor.addOption("md", "minDF",
				"(Optional) The minimum document frequency.  Default is 1",
				true);

		String maxDFPercentOpt = cliProcessor
				.addOption(
						"x",
						"maxDFPercent",
						"(Optional) The max percentage of docs for the DF.  Can be used to remove really high frequency terms."
								+ " Expressed as an integer between 0 and 100. Default is 99.",
						true);

		String minLLROpt = cliProcessor.addOption("ml", "minLLR",
				"(Optional) The minimum Log Likelihood Ratio(Float)  Default is "
						+ LLRReducer.DEFAULT_MIN_LLR, true);

		String numReduceTasksOpt = cliProcessor.addOption("nr", "numReducers",
				"(Optional) Number of reduce tasks. Default Value: 1", true);

		String powerOpt = cliProcessor
				.addOption(
						"n",
						"norm",
						"(Optional) The norm to use, expressed as either a float or \"INF\" if you want to use the Infinite norm.  "
								+ "Must be greater or equal to 0.  The default is not to normalize",
						true);

		String logNormalizeOpt = cliProcessor
				.addOption(
						"lnorm",
						"logNormalize",
						"(Optional) Whether output vectors should be logNormalize. If set true else false",
						true);

		String maxNGramSizeOpt = cliProcessor.addOption("ng", "maxNGramSize",
				"(Optional) The maximum size of ngrams to create"
						+ " (2 = bigrams, 3 = trigrams, etc) Default Value:1",
				true);

		String sequentialAccessVectorOpt = cliProcessor
				.addOption(
						"seq",
						"sequentialAccessVector",
						"(Optional) Whether output vectors should be SequentialAccessVectors. If set true else false",
						false);

		String namedVectorOpt = cliProcessor
				.addOption(
						"nv",
						"namedVector",
						"(Optional) Whether output vectors should be NamedVectors. If set true else false",
						false);

		String overwriteOutput = cliProcessor.addOption("ow", "overwrite",
				"If set, overwrite the output directory", false);

		try {
			cliProcessor.parse(args);
		} catch (ParseException e) {
			return;
		}
		
		try{
			Path inputDir = new Path(cliProcessor.getInputValue());
			Path outputDir = new Path(cliProcessor.getOutputValue());

			int chunkSize = cliProcessor.getIntArgument(chunkSizeOpt, 100);
			int minSupport = cliProcessor.getIntArgument(minSupportOpt, 2);
			int maxNGramSize = cliProcessor.getIntArgument(maxNGramSizeOpt, 1);
			log.info("Maximum n-gram size is: {}", maxNGramSize);

			if (cliProcessor.hasOption(overwriteOutput)) {
				HadoopUtil.overwriteOutput(outputDir);
			}

			float minLLRValue = LLRReducer.DEFAULT_MIN_LLR;
			if (cliProcessor.getOptionValue(minLLROpt) != null) {
				minLLRValue = Float.parseFloat(cliProcessor
						.getOptionValue(minLLROpt));
			}
			log.info("Minimum LLR value: {}", minLLRValue);

			int reduceTasks = cliProcessor.getIntArgument(numReduceTasksOpt, 1);
			log.info("Number of reduce tasks: {}", reduceTasks);

			String type = cliProcessor.getOptionValue(typeNameOpt);
			String featureName = cliProcessor.getOptionValue(featureNameOpt);
			log.info("Getting tokens from " + type + "." + featureName);

			boolean processIdf;
			if (cliProcessor.getOptionValue(weightOpt) != null) {
				String wString = cliProcessor.getOptionValue(weightOpt);
				if ("tf".equalsIgnoreCase(wString)) {
					processIdf = false;
				} else if ("tfidf".equalsIgnoreCase(wString)) {
					processIdf = true;
				} else {
					throw new ParseException(weightOpt);
				}
			} else {
				processIdf = true;
			}

			int minDf = cliProcessor.getIntArgument(minDFOpt, 1);
			int maxDFPercent = cliProcessor.getIntArgument(maxDFPercentOpt, 99);

			float norm = PartialVectorMerger.NO_NORMALIZING;
			if (cliProcessor.getOptionValue(powerOpt) != null) {
				String power = cliProcessor.getOptionValue(powerOpt);
				if ("INF".equals(power)) {
					norm = Float.POSITIVE_INFINITY;
				} else {
					norm = Float.parseFloat(power);
				}
			}

			boolean logNormalize = false;
			if (cliProcessor.getOptionValue(logNormalizeOpt) != null) {
				logNormalize = true;
			}

			HadoopUtil.overwriteOutput(outputDir);
			Path tokenizedPath = new Path(outputDir,
					DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

			DocumentProcessor.tokenizeDocuments(inputDir, type, featureName,
					tokenizedPath);

			boolean sequentialAccessOutput = cliProcessor
					.hasOption(sequentialAccessVectorOpt);
			boolean namedVectors = cliProcessor.hasOption(namedVectorOpt);

			Configuration conf = new Configuration();
			if (!processIdf) {
				DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
						outputDir, conf, minSupport, maxNGramSize, minLLRValue,
						norm, logNormalize, reduceTasks, chunkSize,
						sequentialAccessOutput, namedVectors);
			} else if (processIdf) {
				DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
						outputDir, conf, minSupport, maxNGramSize, minLLRValue,
						-1.0f, false, reduceTasks, chunkSize,
						sequentialAccessOutput, namedVectors);

				TFIDFConverter.processTfIdf(new Path(outputDir,
						DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
						outputDir, chunkSize, minDf, maxDFPercent, norm,
						logNormalize, sequentialAccessOutput, namedVectors,
						reduceTasks);
			}
		} catch (ParseArgumentException ex) {
			System.err.println("Could not parse " + ex.getOption()
					+ " with value " + ex.getValue());
			cliProcessor.showUsage();
		}
	}
}