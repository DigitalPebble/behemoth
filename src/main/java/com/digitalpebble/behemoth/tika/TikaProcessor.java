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
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.utils.ParseUtils;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;
import com.digitalpebble.behemoth.gate.GATEProcessor;

/**
 * Tika as a document processor. Extracts the text and metadata from the
 * original content + converts the XHTML tags into annotations.
 **/

public class TikaProcessor implements DocumentProcessor {

  private static final Log LOG = LogFactory.getLog(GATEProcessor.class);

  private Configuration config;

  private MimeTypes mimetypes = TikaConfig.getDefaultConfig()
      .getMimeRepository();

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf(Configuration conf) {
    config = conf;
  }

  @Override
  public void close() {
  }

  /**
   * Process a BehemothDocument with Tika
   * 
   * @return an array of documents or null if an exception is encountered
   **/
  @Override
  public BehemothDocument[] process(BehemothDocument inputDoc, Reporter reporter) {
    // check that it has some text or content
    if (inputDoc.getContent() == null && inputDoc.getText() == null) {
      LOG.info("No content or text for " + inputDoc.getUrl() + " skipping");
      return null;
    }

    // determine the content type if missing
    if (inputDoc.getContentType() == null) {
      String mt = null;
      // using the original content
      if (inputDoc.getContent() != null) {
        MimeType mimetype = mimetypes.getMimeType(inputDoc.getUrl(), inputDoc
            .getContent());
        mt = mimetype.getName();
      } else if (inputDoc.getText() != null) {
        // force it to text
        mt = "text/plain";
      }
      if (mt != null)
        inputDoc.setContentType(mt);
    }
    
    // TODO extract the text AND the annotations
    
    // does the input document have a some text?
    // if not use Tika to extract it
    if (inputDoc.getText() == null) {
      // convert binary content into Gate doc
      InputStream is = new ByteArrayInputStream(inputDoc.getContent());
      String textContent;
      try {
        textContent = ParseUtils.getStringContent(is, TikaConfig
            .getDefaultConfig(), inputDoc.getContentType());
      } catch (Exception e) {
        LOG.error(inputDoc.getUrl().toString(), e);
        return null;
      }
      if (textContent != null)
        inputDoc.setText(textContent);
    }
    
    // TODO if the content type is an archive maybe process and return 
    // all the subdocuments
    
    return new BehemothDocument[] { inputDoc };
  }

}
