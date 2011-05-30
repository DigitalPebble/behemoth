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
package com.digitalpebble.behemoth.tika;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Uses a {@link com.digitalpebble.behemoth.tika.TikaProcessor} to extract text
 * using Tika. Users wanting to override the default work of the TikaProcessor
 * can set the "tika.processor" value in the JobConf and give it a fully
 * qualified class name. The implementation must extend TikaProcessor and it
 * must have a zero arg. constructor.
 */
public class TikaMapper extends Mapper<Text, BehemothDocument, Text, BehemothDocument> {
    private static final Logger LOG = LoggerFactory.getLogger(TikaMapper.class);

    protected TikaProcessor processor;

    @Override
    public void map(Text text, BehemothDocument inputDoc,
           Mapper<Text, BehemothDocument, Text, BehemothDocument>.Context context) throws IOException, InterruptedException {

        BehemothDocument[] documents = processor.process(inputDoc, context);
        if (documents != null) {
        	for (BehemothDocument doc : documents) {
                context.write(text, doc);
            }
        }
    }

    @Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String handlerName = conf.get("tika.processor");
		if (handlerName != null) {
			Class handlerClass = conf
					.getClass(handlerName, TikaProcessor.class);
			try {
				processor = (TikaProcessor) handlerClass.newInstance();

			} catch (InstantiationException e) {
				LOG.error("Exception", e);
				// TODO: what's the best way to do this?
				throw new RuntimeException(e);
			} catch (IllegalAccessException e) {
				LOG.error("Exception", e);
				throw new RuntimeException(e);
			}
		} else {
			processor = new TikaProcessor();
		}
		processor.setConf(conf);
	}
}
