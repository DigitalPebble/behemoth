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

import java.io.IOException;
import java.net.URLEncoder;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentFilter;

/**
 * Stores the content from Behemoth documents into a local directory
 **/
public class ContentExtractor extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory
            .getLogger(ContentExtractor.class);

    public enum FileNamingMode {
        URL, UUID, NUM
    }

    private FileNamingMode mode = FileNamingMode.UUID;

    // dump the text otherwise
    private boolean dumpBinary = false;

    public ContentExtractor() {
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new ContentExtractor(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Options options = new Options();
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        // create the parser
        CommandLineParser parser = new GnuParser();

        options.addOption("h", "help", false, "print this message");
        options.addOption("i", "input", true, "Behemoth corpus");
        options.addOption("o", "output", true, "local corpus dir");
        options.addOption("b", "binary", false, "dumps binary content, text otherwise");

        // parse the command line arguments
        try {
            CommandLine line = parser.parse(options, args);
            String input = line.getOptionValue("i");
            String output = line.getOptionValue("o");
            if (line.hasOption("help")) {
                formatter.printHelp("ContentExtractor", options);
                return 0;
            }
            if (input == null || output == null) {
                formatter.printHelp("ContentExtractor", options);
                return -1;
            }
            dumpBinary = line.hasOption("binary");
            generateDocs(input, output);

        } catch (ParseException e) {
            formatter.printHelp("ContentExtractor", options);
        }
        return 0;
    }

    private void generateDocs(String inputf, String outputf) throws IOException {
        
        Path input = new Path(inputf);
        Path dirPath = new Path(outputf);

        FileSystem fsout = FileSystem.get(dirPath.toUri(), getConf());

        if (fsout.exists(dirPath) == false)
            fsout.mkdirs(dirPath);
        else if (fsout.isFile(dirPath)) {
            System.err.println("Output " + outputf
                    + " already exists as a file!");
            return;
        }

        FileSystem fs = input.getFileSystem(getConf());
        FileStatus[] statuses = fs.listStatus(input);
        int count[] = { 0 };
        for (int i = 0; i < statuses.length; i++) {
            FileStatus status = statuses[i];
            Path suPath = status.getPath();
            if (suPath.getName().equals("_SUCCESS"))
                continue;
            generateDocs(suPath, dirPath, count);
        }
    }

    private void generateDocs(Path input, Path dir, int[] count)
            throws IOException {

        DocumentFilter docFilter = DocumentFilter.getFilters(getConf());

        FileSystem fsout = FileSystem.get(dir.toUri(), getConf());

        Reader[] cacheReaders = SequenceFileOutputFormat.getReaders(getConf(),
                input);
        for (Reader current : cacheReaders) {
            // read the key + values in that file
            Text key = new Text();
            BehemothDocument inputDoc = new BehemothDocument();
            FSDataOutputStream out = null;
            while (current.next(key, inputDoc)) {
                count[0]++;
                // filter the doc?
                if (!docFilter.keep(inputDoc))
                    continue;
                if (dumpBinary && inputDoc.getContent() == null)
                    continue;
                else if (!dumpBinary && inputDoc.getText() == null)
                    continue;
                try {
                    String fileName = Integer.toString(count[0]);
                    String urldoc = inputDoc.getUrl();
                    if (mode.equals(FileNamingMode.URL) && urldoc != null
                            && urldoc.length() > 0) {
                        fileName = URLEncoder.encode(urldoc, "UTF-8");
                    }
                    if (mode.equals(FileNamingMode.UUID) && urldoc != null
                            && urldoc.length() > 0) {
                        fileName = UUID.nameUUIDFromBytes(urldoc.getBytes())
                                .toString();
                    } else {
                        fileName = String.format("%08d", count[0]);
                    }

                    if (!dumpBinary)
                        fileName += ".txt";

                    Path outFilePath = new Path(dir, fileName);
                    if (fsout.exists(outFilePath) == false) {
                        fsout.createNewFile(outFilePath);
                    } else {
                        // already there! prefix with global counter
                        outFilePath = new Path(dir, Integer.toString(count[0])
                                + "_" + fileName);
                        if (fsout.exists(outFilePath) == false) {
                            fsout.createNewFile(outFilePath);
                        }
                    }

                    out = fsout.create(outFilePath);

                    byte[] contentBytes;
                    if (dumpBinary)
                        contentBytes = inputDoc.getContent();
                    else
                        contentBytes = inputDoc.getText().getBytes("UTF-8");

                    out.write(contentBytes, 0, contentBytes.length);

                } catch (Exception e) {
                    LOG.error(
                            "Exception on doc [" + count[0] + "] "
                                    + key.toString(), e);
                } finally {
                    if (out != null)
                        out.close();
                }
            }
            current.close();
        }
    }
}
