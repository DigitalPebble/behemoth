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
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringTuple;

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Extracts tokens from a Behemoth document and outputs them in a StringTuple
 */
public class SequenceFileTokenizerMapper extends
        Mapper<Text, BehemothDocument, Text, StringTuple> {

    private String tokenType;
    private String tokenFeature;

    @Override
    protected void map(Text key, BehemothDocument value, Context context)
            throws IOException, InterruptedException {
        StringTuple document = new StringTuple();
        Iterator<Annotation> iter = value.getAnnotations().iterator();

        while (iter.hasNext()) {
            Annotation annot = iter.next();
            // check the type
            if (!annot.getType().equals(tokenType))
                continue;
            java.util.Map<String, String> features = annot.getFeatures();
            if (features == null)
                continue;
            String featureValue = features.get(tokenFeature);
            if (featureValue == null)
                continue;
            document.add(featureValue);

        }
        context.write(key, document);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        this.tokenType = context.getConfiguration()
                .get(DocumentProcessor.TOKEN_TYPE);
        this.tokenFeature = context.getConfiguration().get(
                DocumentProcessor.FEATURE_NAME);
    }
}