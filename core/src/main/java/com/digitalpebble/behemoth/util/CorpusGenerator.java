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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Generates a SequenceFile containing BehemothDocuments given a local
 * directory. The BehemothDocument gets its byte content and URL. The detection
 * of MIME-type and text extraction can be done later using the TikaProcessor.
 */

public class CorpusGenerator extends Configured implements Tool {
    private transient static Logger log = LoggerFactory
            .getLogger(CorpusGenerator.class);
    private Path input, output;

    private Reporter reporter;

    private static String unpackParamName = "CorpusGenerator-unpack";

    public enum Counters {
        DOC_COUNT
    };

    public CorpusGenerator() {
    }

    public CorpusGenerator(Path input, Path output) {
        setInput(input);
        setOutput(output);
    }

    public CorpusGenerator(Path input, Path output, Reporter reporter) {
        this.input = input;
        this.output = output;
        this.reporter = reporter;
    }

    public void setInput(Path input) {
        this.input = input;
    }

    public void setOutput(Path output) {
        this.output = output;
    }

    public long generate(boolean recurse) throws IOException {
        long result = 0;
        // read from input path
        // create new Content object and add it to the SequenceFile
        Text key = new Text();
        BehemothDocument value = new BehemothDocument();
        SequenceFile.Writer writer = null;
        try {
            Configuration conf = getConf();
            FileSystem fs = output.getFileSystem(conf);
            writer = SequenceFile.createWriter(fs, conf, output,
                    key.getClass(), value.getClass());
            PerformanceFileFilter pff = new PerformanceFileFilter(writer, key,
                    value, conf, reporter);
            // iterate on the files in the source dir
            result = processFiles(conf, input, recurse, pff);

        } finally {
            IOUtils.closeStream(writer);
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new CorpusGenerator(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Options options = new Options();
        // automatically generate the help statement
        HelpFormatter formatter = new HelpFormatter();
        // create the parser
        CommandLineParser parser = new GnuParser();

        options.addOption("h", "help", false, "print this message");
        options.addOption("i", "input", true, "input file or directory");
        options.addOption("o", "output", true, "output Behemoth corpus");
        options.addOption("r", "recurse", true,
                "processes directories recursively (default true)");
        options.addOption("u", "unpack", true,
                "unpack content of archives (default true)");

        // parse the command line arguments
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            String input = line.getOptionValue("i");
            if (line.hasOption("help")) {
                formatter.printHelp("CorpusGenerator", options);
                return 0;
            }
            if (input == null) {
                formatter.printHelp("CorpusGenerator", options);
                return -1;
            }
        } catch (ParseException e) {
            formatter.printHelp("CorpusGenerator", options);
        }

        boolean recurse = true;
        if ("false".equalsIgnoreCase(line.getOptionValue("r")))
            recurse = false;
        boolean unpack = true;
        if ("false".equalsIgnoreCase(line.getOptionValue("u")))
            unpack = false;

        getConf().setBoolean(unpackParamName, unpack);

        Path inputDir = new Path(line.getOptionValue("i"));
        Path output = new Path(line.getOptionValue("o"));

        setInput(inputDir);
        setOutput(output);

        long count = generate(recurse);
        System.out.println(count + " docs converted");
        return 0;
    }

    private static long processFiles(Configuration conf, Path input,
            boolean recurse, PerformanceFileFilter pff) throws IOException {

        FileSystem fs = input.getFileSystem(conf);
        FileStatus[] statuses = fs.listStatus(input, pff);
        for (int i = 0; i < statuses.length; i++) {
            FileStatus status = statuses[i];
            if (recurse == true) {
                processFiles(conf, status.getPath(), recurse, pff);
            }
        }
        return pff.counter;
    }

    // Java hack to move the work of processing files into a filter, so that we
    // can process large directories of files
    // without having to create a huge list of files
    static class PerformanceFileFilter implements PathFilter {
        long counter = 0;
        PathFilter defaultIgnores = new PathFilter() {

            public boolean accept(Path file) {
                String name = file.getName();
                return name.startsWith(".") == false;// ignore hidden
                // directories
            }
        };

        private SequenceFile.Writer writer;
        private Text key;
        private BehemothDocument value;
        private Configuration conf;
        private Reporter reporter;

        public PerformanceFileFilter(SequenceFile.Writer writer, Text key,
                BehemothDocument value, Configuration conf, Reporter reporter) {
            this.writer = writer;
            this.key = key;
            this.value = value;
            this.conf = conf;
            this.reporter = reporter;
        }

        public boolean accept(Path file) {
            try {
                FileSystem fs = file.getFileSystem(conf);
                boolean unpack = conf.getBoolean(unpackParamName, true);

                if (defaultIgnores.accept(file)
                        && fs.getFileStatus(file).isDir() == false) {
                    String URI = file.toUri().toString();

                    // detect whether a file is likely to be an archive
                    // TODO extend to other known types
                    if (unpack && URI.toLowerCase().endsWith(".zip")) {
                        FSDataInputStream fis = null;
                        try {
                            fis = fs.open(file);
                            ArchiveInputStream input = new ArchiveStreamFactory()
                                    .createArchiveInputStream(new BufferedInputStream(
                                            fis));
                            ArchiveEntry entry = null;
                            while ((entry = input.getNextEntry()) != null) {
                                String name = entry.getName();
                                long size = entry.getSize();
                                byte[] content = new byte[(int) size];
                                input.read(content);
                                key.set(name);
                                // fill the values for the content object
                                value.setUrl(name);
                                value.setContent(content);

                                writer.append(key, value);
                                counter++;
                                if (reporter != null) {
                                    reporter.incrCounter(Counters.DOC_COUNT, 1);
                                }
                            }

                        } catch (ArchiveException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } finally {
                            fis.close();
                        }

                    } else {
                        // Hmm, kind of dangerous to do this
                        byte[] fileBArray = new byte[(int) fs.getFileStatus(
                                file).getLen()];
                        FSDataInputStream fis = null;
                        try {
                            fis = fs.open(file);
                            fis.readFully(0, fileBArray);
                            fis.close();
                            key.set(URI);
                            // fill the values for the content object
                            value.setUrl(URI);
                            value.setContent(fileBArray);

                            writer.append(key, value);
                            counter++;
                            if (reporter != null) {
                                reporter.incrCounter(Counters.DOC_COUNT, 1);
                            }
                        } catch (FileNotFoundException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                // if it is a directory, accept it so we can possibly recurse on
                // it,
                // otherwise we don't care about actually accepting the file,
                // since
                // all the work is done in the accept method here.
                return fs.getFileStatus(file).isDir();
            } catch (IOException e) {
                log.error("Exception", e);
            }
            return false;
        }
    }

}