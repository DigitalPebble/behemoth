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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentFilter;

/**
 * Utility class used to read the content of a Behemoth SequenceFile.
 **/
public class CorpusReader extends Configured implements Tool {

    private static final String CORPUS_READER = "CorpusReader";

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new CorpusReader(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Options options = new Options();
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        // create the parser
        CommandLineParser parser = new GnuParser();

        options.addOption("h", "help", false, "print this message");
        options.addOption("i", "input", true, "input Behemoth corpus");
        options.addOption("c", "displayContent", false,
                "display binary content in output");
        options.addOption("t", "displayText", false, "display text in output");
        options.addOption("a", "displayAnnotations", false,
                "display annotations in output");
        options.addOption("m", "displayMetadata", false,
                "display metadata in output");

        // parse the command line arguments
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            String input = line.getOptionValue("i");
            if (line.hasOption("help")) {
                formatter.printHelp(CORPUS_READER, options);
                return 0;
            }
            if (input == null) {
                formatter.printHelp(CORPUS_READER, options);
                return -1;
            }
        } catch (ParseException e) {
            formatter.printHelp(CORPUS_READER, options);
            return -1;
        }

        boolean showBinaryContent = line.hasOption("displayContent");
        boolean showText = line.hasOption("displayText");
        boolean showAnnotations = line.hasOption("displayAnnotations");
        boolean showMD = line.hasOption("displayMetadata");

        Path inputPath = new Path(line.getOptionValue("i"));

        Configuration conf = getConf();
        FileSystem fs = inputPath.getFileSystem(conf);

        // filter input
        DocumentFilter filters = DocumentFilter.getFilters(conf);
        boolean doFilter = DocumentFilter.isRequired(conf);

        FileStatus[] fss = fs.listStatus(inputPath);
        for (FileStatus status : fss) {
            Path path = status.getPath();
            // skips the _log or _SUCCESS files
            if (!path.getName().startsWith("part-")
                    && !path.getName().equals(inputPath.getName()))
                continue;
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
            Text key = new Text();
            BehemothDocument value = new BehemothDocument();
            while (reader.next(key, value)) {
                // skip this document?
                if (doFilter && filters.keep(value) == false)
                    continue;

                System.out.println(value.toString(showBinaryContent,
                        showAnnotations, showText, showMD));
            }
            reader.close();
        }

        return 0;
    }
}
