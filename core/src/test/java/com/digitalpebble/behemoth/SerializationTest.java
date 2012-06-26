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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SerializationTest extends TestCase {

	private Configuration conf;
	private FileSystem fs;
	private Path file;

	@Override
	protected void setUp() throws Exception {
		conf = BehemothConfiguration.create();
		fs = FileSystem.getLocal(conf);
		file = new Path("test_" + System.currentTimeMillis());
	}

	@Override
	protected void tearDown() throws Exception {
		fs.close();
	}

	public void testSerialization() throws IOException {
		BehemothDocument doc = new BehemothDocument();
		doc.setUrl("test");
		String tcontent = "This is home";
		doc.setContent(tcontent.getBytes());
		doc.setText(tcontent);
		doc.setContentType("txt");
		Annotation annot = new Annotation();
		annot.setStart(0);
		annot.setEnd(12);
		annot.setType("annotType");
		doc.getAnnotations().add(annot);

		Writer writer = SequenceFile.createWriter(fs, conf, file, Text.class,
				BehemothDocument.class);
		writer.append(new Text("test"), doc);
		writer.close();

		Reader reader = new org.apache.hadoop.io.SequenceFile.Reader(fs,file,conf);
		Text key2 = new Text();
		BehemothDocument doc2 = new BehemothDocument();
		reader.next(key2, doc2);
		reader.close();
		
		fs.delete(file,true);
		
		// check the values 
	}

}
