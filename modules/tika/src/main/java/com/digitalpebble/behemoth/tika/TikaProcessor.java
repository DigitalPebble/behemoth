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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.HtmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;

/**
 * Tika as a document processor. Extracts the text and metadata from the
 * original content + converts the XHTML tags into annotations.
 */

public class TikaProcessor implements DocumentProcessor, TikaConstants {

    private static final Logger LOG = LoggerFactory
            .getLogger(TikaProcessor.class);

    private Configuration config;

    private String mimeType = "text/plain";

    private MimeTypes mimetypes = TikaConfig.getDefaultConfig()
            .getMimeRepository();

    @Override
    public Configuration getConf() {
        return config;
    }

    @Override
    public void setConf(Configuration conf) {
        config = conf;
        mimeType = config.get(TIKA_MIME_TYPE_KEY);
    }

    @Override
    public void close() {
    }

    /**
     * Process a BehemothDocument with Tika
     * 
     * @return an array of documents or null if an exception is encountered
     */
    @Override
    public BehemothDocument[] process(BehemothDocument inputDoc,
            Reporter reporter) {
        // check that it has some text or content
        if (inputDoc.getContent() == null && inputDoc.getText() == null) {
            LOG.info("No content or text for " + inputDoc.getUrl()
                    + " skipping");
            return null;
        }

        // determine the content type if missing
        if (inputDoc.getContentType() == null
                || inputDoc.getContentType().equals("") == true) {
            String mt = null;
            // using the original content
            if (mimeType == null) {
                if (inputDoc.getContent() != null) {
                    MimeType mimetype = mimetypes.getMimeType(
                            inputDoc.getUrl(), inputDoc.getContent());
                    mt = mimetype.getName();
                } else if (inputDoc.getText() != null) {
                    // force it to text
                    mt = "text/plain";
                }
            } else {
                mt = mimeType;// allow outside user to specify a mime type if
                // they know all the content, saves time and
                // reduces error
            }
            if (mt != null) {
                inputDoc.setContentType(mt);
            }
        }

        // determine which parser to use
        Parser parser = TikaConfig.getDefaultConfig().getParser(); 

        // skip the processing if the input document already has some text
        if (inputDoc.getText() != null)
            return new BehemothDocument[] { inputDoc };

        // otherwise parse the document and retrieve the text, metadata and
        // markup annotations

        InputStream is = new ByteArrayInputStream(inputDoc.getContent());

        Metadata metadata = new Metadata();
        // put the mimetype in the metadata so that Tika can
        // decide which parser to use
        metadata.set(Metadata.CONTENT_TYPE, inputDoc.getContentType());

        if (reporter != null)
            reporter.getCounter("MIME-TYPE", inputDoc.getContentType())
                    .increment(1);

        TikaMarkupHandler handler = new TikaMarkupHandler();
        ParseContext context = new ParseContext();
        // specify a custom HTML mapper via the Context
        context.set(HtmlMapper.class, new IdentityHtmlMapper());

        try {
            parser.parse(is, handler, metadata, context);
            processText(inputDoc, handler.getText());
            processMetadata(inputDoc, metadata);
            processMarkupAnnotations(inputDoc, handler.getAnnotations());
        } catch (Exception e) {
            LOG.error(inputDoc.getUrl().toString(), e);
            if (reporter != null)
                reporter.getCounter("TIKA", "PARSING_ERROR").increment(1);
            return new BehemothDocument[] { inputDoc };
        } finally {
            try {
                is.close();
            } catch (IOException e) {
            }
        }

        // TODO if the content type is an archive maybe process and return
        // all the subdocuments
        if (reporter != null)
            reporter.getCounter("TIKA", "DOC-PARSED").increment(1);

        return new BehemothDocument[] { inputDoc };
    }

    /**
     * Classes that wish to handle how text is processed may override this
     * method, otherwise it just calls
     * {@link com.digitalpebble.behemoth.BehemothDocument#setText(String)}
     * 
     * @param inputDoc
     * @param textContent
     */
    protected void processText(BehemothDocument inputDoc, String textContent) {
        if (textContent != null)
            inputDoc.setText(textContent);
    }

    /**
     * Classes that wish to handle Markup annotations separately may override
     * this method
     * 
     * @param annotations
     *            the markup {@link com.digitalpebble.behemoth.Annotations}
     */
    protected void processMarkupAnnotations(BehemothDocument inputDoc,
            List<com.digitalpebble.behemoth.Annotation> annotations) {
        inputDoc.getAnnotations().addAll(annotations);
    }

    /**
     * Classes that wish to handle Metadata separately may override this method
     * 
     * @param metadata
     *            the extracted {@link org.apache.tika.metadata.Metadata}
     */
    protected void processMetadata(BehemothDocument inputDoc, Metadata metadata) {
        MapWritable mapW = new MapWritable();
        for (String name : metadata.names()) {
            String[] values = metadata.getValues(name);
            // temporary fix to avoid
            // Exception in thread "main" java.io.IOException: can't find class:
            // com.digitalpebble.behemoth.tika.TextArrayWritable because
            // com.digitalpebble.behemoth.tika.TextArrayWritable
            // at
            // org.apache.hadoop.io.AbstractMapWritable.readFields(AbstractMapWritable.java:204)
            // simply store multiple values as a , separated Text
            StringBuffer buff = new StringBuffer();
            for (int i = 0; i < values.length; i++) {
                if (i > 0)
                    buff.append(",");
                buff.append(values[i]);
            }
            mapW.put(new Text(name), new Text(buff.toString()));
            // mapW.put(new Text(name), new TextArrayWritable(values));
        }
        inputDoc.setMetadata(mapW);
    }

}
