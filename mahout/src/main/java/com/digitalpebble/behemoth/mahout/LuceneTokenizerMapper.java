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
package com.digitalpebble.behemoth.mahout;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.Version;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.vectorizer.DocumentProcessor;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Tokenizes a text document and outputs tokens in a StringTuple
 */
public class LuceneTokenizerMapper extends
        Mapper<Text, BehemothDocument, Text, StringTuple> {

    private Analyzer analyzer;
    
    private Version matchVersion = Version.LUCENE_46;

    @Override
    protected void map(Text key, BehemothDocument value, Context context)
            throws IOException, InterruptedException {
        String sContent = value.getText();
        if (sContent == null) {
            // no text available? skip
            context.getCounter("LuceneTokenizer", "BehemothDocWithoutText")
                    .increment(1);
            return;
        }
        analyzer = new StandardAnalyzer(matchVersion); // or any other analyzer
        TokenStream ts = analyzer.tokenStream(key.toString(), new StringReader(sContent));
        // The Analyzer class will construct the Tokenizer, TokenFilter(s), and CharFilter(s),
        //   and pass the resulting Reader to the Tokenizer.
        @SuppressWarnings("unused")
        OffsetAttribute offsetAtt = ts.addAttribute(OffsetAttribute.class);

        CharTermAttribute termAtt = ts
                .addAttribute(CharTermAttribute.class);
        StringTuple document = new StringTuple();
        try {
            ts.reset(); // Resets this stream to the beginning. (Required)
            while (ts.incrementToken()) {
                if (termAtt.length() > 0) {
                    document.add(new String(termAtt.buffer(), 0, termAtt.length()));
                }
            }
            ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
        } finally {
            ts.close(); // Release resources associated with this stream.
      }
        context.write(key, document);
    }

    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        super.setup(context);
        analyzer = ClassUtils.instantiateAs(
                context.getConfiguration().get(
                        DocumentProcessor.ANALYZER_CLASS,
                        StandardAnalyzer.class.getName()), Analyzer.class); 
    }
}
