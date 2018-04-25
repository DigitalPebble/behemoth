package com.digitalpebble.behemoth.io.warc;

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

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.protocol.ProtocolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentFilter;

import edu.cmu.lemurproject.WarcFileInputFormat;
import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

/**
 * Converts a WARC archive into a Behemoth datastructure for further processing
 */
public class WARCConverterJob extends Configured
		implements Tool, Mapper<LongWritable, WritableWarcRecord, Text, BehemothDocument> {

	public static final Logger LOG = LoggerFactory.getLogger(WARCConverterJob.class);

	private Text newKey = new Text();

	private DocumentFilter filter;

	public WARCConverterJob() {
		this(null);
	}

	public WARCConverterJob(Configuration conf) {
		super(conf);
	}

	public void configure(JobConf job) {
		setConf(job);
		filter = DocumentFilter.getFilters(job);
	}

	public void close() {
	}

	public void map(LongWritable key, WritableWarcRecord record, OutputCollector<Text, BehemothDocument> output,
			Reporter reporter) throws IOException {

		WarcRecord wr = record.getRecord();

		if (wr.getHeaderRecordType().equals("response") == false)
			return;

		byte[] binarycontent = wr.getContent();

		String uri = wr.getHeaderMetadataItem("WARC-Target-URI");

		// skip non http documents
		if (uri.startsWith("http") == false)
			return;

		String ip = wr.getHeaderMetadataItem("WARC-IP-Address");

		HttpResponse response;
		try {
			response = new HttpResponse(binarycontent);
		} catch (ProtocolException e) {
			return;
		}

		BehemothDocument behemothDocument = new BehemothDocument();

		behemothDocument.setUrl(uri);
		newKey.set(uri);

		String contentType = response.getHeader(HttpHeaders.CONTENT_TYPE);
		behemothDocument.setContentType(contentType);
		behemothDocument.setContent(response.getContent());

		MapWritable md = behemothDocument.getMetadata(true);

		// add the metadata
		for (String mdkey : response.getHeaders().names()) {
			String value = response.getHeaders().get(mdkey);
			md.put(new Text(mdkey), new Text(value));
		}

		// add the metadata
		String customMetadata = getConf().get("md", "").trim();

		if (customMetadata.isEmpty() == false) {
			String[] mds = customMetadata.split(";");
			for (String metadata : mds) {
				String[] keyval = metadata.split("=");
				LOG.trace("key: {}\tval: {}", keyval[0], keyval[1]);
				Writable mdvalue;
				Writable mdkey = new Text(keyval[0]);
				if (keyval.length == 1) {
					mdvalue = NullWritable.get();
				} else {
					mdvalue = new Text(keyval[1]);
				}
				md.put(mdkey, mdvalue);
			}
		}

		// store the IP address as metadata
		if (StringUtils.isNotBlank(ip))
			md.put(new Text("IP"), new Text(ip));

		if (filter.keep(behemothDocument)) {
			output.collect(newKey, behemothDocument);
			reporter.getCounter("WARCCONVERTER", "KEPT").increment(1l);
		} else
			reporter.getCounter("WARCCONVERTER", "FILTERED").increment(1l);

	}

	public int convert(Path warcpath, Path output) throws IOException {

		JobConf job = new JobConf(getConf());
		job.setJobName("Convert WARC " + warcpath);

		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(job, warcpath);
		job.setInputFormat(WarcFileInputFormat.class);

		job.setMapperClass(WARCConverterJob.class);

		// no reducers
		job.setNumReduceTasks(0);

		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BehemothDocument.class);

		long start = System.currentTimeMillis();
		boolean success = JobClient.runJob(job).isSuccessful();
		long finish = System.currentTimeMillis();
		if (LOG.isInfoEnabled()) {
			LOG.info("WARCConverterJob completed. Timing: " + (finish - start) + " ms");
		}
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(BehemothConfiguration.create(), new WARCConverterJob(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Options options = new Options();
		// automatically generate the help statement
		HelpFormatter formatter = new HelpFormatter();
		// create the parser
		CommandLineParser parser = new GnuParser();

		options.addOption("h", "help", false, "print this message");
		options.addOption("i", "input", true, "input WARC file");
		options.addOption("o", "output", true, "output Behemoth corpus");
		options.addOption("md", "metadata", true,
				"add document metadata separated by semicolon e.g. -md source=internet;label=public");

		// parse the command line arguments
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
			if (line.hasOption("help")) {
				formatter.printHelp("WARCConverterJob", options);
				return 0;
			}
			if (!line.hasOption("i")) {
				formatter.printHelp("WARCConverterJob", options);
				return -1;
			}
			if (!line.hasOption("o")) {
				formatter.printHelp("WARCConverterJob", options);
				return -1;
			}
		} catch (ParseException e) {
			formatter.printHelp("WARCConverterJob", options);
		}

		Path input = new Path(line.getOptionValue("i"));
		Path output = new Path(line.getOptionValue("o"));

		if (line.hasOption("md")) {
			String md = line.getOptionValue("md");
			getConf().set("md", md);
		}

		return convert(input, output);
	}

}
