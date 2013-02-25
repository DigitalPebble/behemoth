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

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;

public class BehemothLabelMapper extends
        Mapper<Text, BehemothDocument, Text, Text> {

    private static final Logger log = LoggerFactory
            .getLogger(BehemothLabelMapper.class);

    private Text metadataKey;

    @Override
    protected void map(Text key, BehemothDocument value, Context context)
            throws IOException, InterruptedException {
        Text label = (Text) value.getMetadata(true).get(metadataKey);
        if (label != null)
            context.write(key, label);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        metadataKey = new Text(context.getConfiguration().get(
                BehemothDocumentProcessor.MD_LABEL, "UNKNOWN_METADATA_KEY"));
        log.info("Using metadata key " + metadataKey + " for labels");
    }
}