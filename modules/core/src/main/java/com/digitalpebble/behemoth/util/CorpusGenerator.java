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

package com.digitalpebble.behemoth.util;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.CliProcessor;

/**
 * Generates a SequenceFile containing BehemothDocuments given a local
 * directory. The BehemothDocument gets its byte content and URL. The detection
 * of MIME-type and text extraction can be done later using the TikaProcessor.
 **/

public class CorpusGenerator {

	public final static String USAGE = "Generate a Behemoth corpus on HDFS from a local directory";
	
    public static void main(String argv[]) throws Exception {

        // Populate a SequenceFile with the content of a local directory
        
		CliProcessor cliProcessor = new CliProcessor(CorpusGenerator.class.getSimpleName(),
				USAGE);
		String inputOpt = cliProcessor.addRequiredOption("i", "input",
				"Input directory on local file system", true);
		String outputOpt = cliProcessor.addRequiredOption("o", "output",
				"Output directory on HDFS", true);
		String recurseOpt = cliProcessor.addOption("s", "recurse",
				"Recurse through input directories", false);

		try {
			cliProcessor.parse(argv);
		} catch (ParseException me) {
			return;
		}
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        File inputDir = new File(cliProcessor.getOptionValue(inputOpt));

        Path output = new Path(cliProcessor.getOptionValue(outputOpt));

        boolean recurse = cliProcessor.hasOption(recurseOpt);
        
        // read from input path
        // create new Content object and add it to the SequenceFile
        Text key = new Text();
        BehemothDocument value = new BehemothDocument();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, output,
                    key.getClass(), value.getClass());
            PerformanceFileFilter pff = new PerformanceFileFilter(writer, key,
                    value);
            // iterate on the files in the source dir
            processFiles(inputDir, recurse, pff);

        } finally {
            IOUtils.closeStream(writer);
        }

    }

    private static void processFiles(File inputDir, boolean recurse,
            PerformanceFileFilter pff) {
        for (File file : inputDir.listFiles(pff)) {
            // handle directories here, as they are the only thing coming back
            // due to the use of the PFF
            if (recurse == true) {
                processFiles(file, recurse, pff);
            }
        }
    }

    // Java hack to move the work of processing files into a filter, so that we
    // can process large directories of files
    // without having to create a huge list of files
    static class PerformanceFileFilter implements FileFilter {

        FileFilter defaultIgnores = new FileFilter() {
            @Override
            public boolean accept(File file) {
                String name = file.getName();
                return name.startsWith(".") == false;// ignore hidden
                // directories
            }
        };

        private SequenceFile.Writer writer;
        private Text key;
        private BehemothDocument value;

        public PerformanceFileFilter(SequenceFile.Writer writer, Text key,
                BehemothDocument value) {
            this.writer = writer;
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean accept(File file) {
            if (defaultIgnores.accept(file) && file.isDirectory() == false) {
                String URI = file.toURI().toString();

                byte[] fileBArray = new byte[(int) file.length()];
                FileInputStream fis = null;
                try {
                    fis = new FileInputStream(file);
                    fis.read(fileBArray);
                    fis.close();
                    key.set(URI);
                    // fill the values for the content object
                    value.setUrl(URI);
                    value.setContent(fileBArray);

                    writer.append(key, value);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            // if it is a directory, accept it so we can possibly recurse on it,
            // otherwise we don't care about actually accepting the file, since
            // all the work is done in the accept method here.
            return file.isDirectory();
        }
    }

}
