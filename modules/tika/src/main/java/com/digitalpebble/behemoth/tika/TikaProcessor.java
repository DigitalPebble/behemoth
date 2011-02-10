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

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Tika as a document processor. Extracts the text and metadata from the
 * original content + converts the XHTML tags into annotations.
 */

public class TikaProcessor implements DocumentProcessor, TikaConstants {

    private static final Logger LOG = LoggerFactory
            .getLogger(TikaProcessor.class);

    private Configuration config;

    private String mimeType = "text/plain";
    private Tika tika = new Tika();
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

        // TODO extract the markup as annotations

        // does the input document have a some text?
        // if not use Tika to extract it
        if (inputDoc.getText() == null) {
            InputStream is = new ByteArrayInputStream(inputDoc.getContent());
            String textContent;
            try {
                Metadata metadata = new Metadata();
                textContent = tika.parseToString(is, metadata);// ParseUtils.getStringContent(is,
                // TikaConfig.getDefaultConfig(),
                // inputDoc.getContentType());
                processText(inputDoc, textContent);
                processMetadata(inputDoc, metadata);
            } catch (Exception e) {
                LOG.error(inputDoc.getUrl().toString(), e);
                return null;
            }
        }
        // TODO if the content type is an archive maybe process and return
        // all the subdocuments

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
