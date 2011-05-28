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

package com.digitalpebble.behemoth.gate;

import gate.Gate;
import gate.util.GateException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.InputOutputCliProcessor;

/**
 * Converts a Behemoth corpus into a XML corpus for GATE. This is used mostly
 * for displaying the documents with the GATE GUI. This is not a mapreduce job.
 **/
public class GATECorpusGenerator extends Configured implements Tool {

    public GATECorpusGenerator() throws GateException {
        Gate.runInSandbox(true);
        Gate.init();
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(BehemothConfiguration.create(),
                new GATECorpusGenerator(), args);
        System.exit(res);
    }

	public int run(String[] args) throws Exception {

		InputOutputCliProcessor cliProcessor = new InputOutputCliProcessor(
				GATECorpusGenerator.class,
				"Converts a Behemoth corpus into a XML corpus for GATE. This is not a Map/Reduce job.");

		try {
			cliProcessor.parse(args);
		} catch (ParseException me) {
			return -1;
		}

		String input = cliProcessor.getOutputValue();
		String output = cliProcessor.getOutputValue();
		generateXMLdocs(input, output);

		return 0;
	}

    private void generateXMLdocs(String inputf, String outputf)
            throws IOException {
        Path input = new Path(inputf);

        File output = new File(outputf);
        if (output.exists() && output.isFile()) {
            System.err.println("Output " + outputf + " already exists");
            return;
        }
        if (output.exists() == false)
            output.mkdirs();

        Reader[] cacheReaders = SequenceFileOutputFormat.getReaders(getConf(),
                input);
        for (Reader current : cacheReaders) {
            // read the key + values in that file
            Text key = new Text();
            BehemothDocument inputDoc = new BehemothDocument();
            int num = 0;
            while (current.next(key, inputDoc)) {
                num++;
                // generate a GATE document then save it to XML
                try {
                    // first put the text
                    gate.Document gatedocument = GATEProcessor
                            .generateGATEDoc(inputDoc);

                    // then save as XML
                    File outputFile = new File(output, num + ".xml");
                    if (outputFile.exists() == false)
                        outputFile.createNewFile();

                    BufferedWriter writer = new BufferedWriter(new FileWriter(
                            outputFile));
                    writer.write(gatedocument.toXml());
                    writer.close();

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            current.close();
        }
    }

}
