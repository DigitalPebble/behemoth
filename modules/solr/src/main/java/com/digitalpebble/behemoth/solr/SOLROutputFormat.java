/*
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
package com.digitalpebble.behemoth.solr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.digitalpebble.behemoth.BehemothDocument;

public class SOLROutputFormat extends FileOutputFormat<Text, BehemothDocument> {

    @Override
    public RecordWriter<Text, BehemothDocument> getRecordWriter(
            TaskAttemptContext context) throws IOException,
            InterruptedException {

        final SOLRWriter writer = new SOLRWriter();
        writer.open(context.getConfiguration(), context.getJobName());

        return new RecordWriter<Text, BehemothDocument>() {

            @Override
            public void close(TaskAttemptContext context) throws IOException {
                writer.close();
            }

            @Override
            public void write(Text key, BehemothDocument doc)
                    throws IOException {
                writer.write(doc);
            }
        };
    }
}
