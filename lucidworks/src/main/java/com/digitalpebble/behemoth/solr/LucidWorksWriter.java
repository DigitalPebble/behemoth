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

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class LucidWorksWriter {

  private static final Log LOG = LogFactory.getLog(LucidWorksWriter.class);

  private SolrServer solr;

  // key = Annotation type ; value = feature name / SOLR field
  private Map<String, Map<String, String>> fieldMapping = new HashMap<String, Map<String, String>>();
  private Progressable progress;

  public LucidWorksWriter(Progressable progress) {
    this.progress = progress;
  }

  public void open(JobConf job, String name) throws IOException {
    String zkHost = job.get("solr.zkhost");
    if (zkHost != null && zkHost.equals("") == false) {
      String collection = job.get("solr.zk.collection", "collection1");
      solr = new CloudSolrServer(zkHost);
      ((CloudSolrServer)solr).setDefaultCollection(collection);
    } else {
      String solrURL = job.get("solr.server.url");
      int queueSize = job.getInt("solr.client.queue.size", 100);
      int threadCount = job.getInt("solr.client.threads", 1);
      solr = new StreamingUpdateSolrServer(solrURL, queueSize, threadCount);
    }
    // get the Behemoth annotations types and features
    // to store as SOLR fields
    // solr.f.name = BehemothType.featureName
    // e.g. solr.f.person = Person.string
    Iterator<Entry<String, String>> iterator = job.iterator();
    while (iterator.hasNext()) {
      Entry<String, String> entry = iterator.next();
      if (entry.getKey().startsWith("solr.f.") == false)
        continue;
      String fieldName = entry.getKey().substring("solr.f.".length());
      String val = entry.getValue();
      // see if a feature has been specified
      // if not we'll use '*' to indicate that we want
      // the text covered by the annotation
      HashMap<String, String> featureValMap = new HashMap<String, String>();
      int separator = val.indexOf(".");
      String featureName = "*";
      if (separator != -1)
        featureName = val.substring(separator + 1);
      featureValMap.put(featureName, fieldName);
      fieldMapping.put(entry.getValue(), featureValMap);
      LOG.debug("Adding to mapping " + entry.getValue() + " "
              + featureName + " " + fieldName);
    }
  }

  public void write(BehemothDocument doc) throws IOException {
    final SolrInputDocument inputDoc = convertToSOLR(doc);
    try {
      progress.progress();
      solr.add(inputDoc);
    } catch (SolrServerException e) {
      throw makeIOException(e);
    }
  }

  protected SolrInputDocument convertToSOLR(BehemothDocument doc) {
    final SolrInputDocument inputDoc = new SolrInputDocument();
    // map from a Behemoth document to a SOLR one
    // the field names below should be modified
    // to match the SOLR schema
    inputDoc.setField("id", doc.getUrl());
    inputDoc.setField("text", doc.getText());

    LOG.info("Adding field : id\t" + doc.getUrl());
    LOG.info("Adding field : text\t" + doc.getText());

    // iterate on the annotations of interest and
    // create a new field for each one
    // it is advised NOT to set frequent annotation types
    // such as token as this would generate a stupidly large
    // number of fields which won't be used by SOLR for
    // tokenizing anyway.
    // what you can do though is to concatenate the token values
    // to form a new content string separated by spaces

    // iterate on the annotations
    Iterator<Annotation> iterator = doc.getAnnotations().iterator();
    while (iterator.hasNext()) {
      Annotation current = iterator.next();
      // check whether it belongs to a type we'd like to send to SOLR
      Map<String, String> featureField = fieldMapping.get(current
              .getType());
      if (featureField == null)
        continue;
      // iterate on the expected features
      for (String targetFeature : featureField.keySet()) {
        String SOLRFieldName = featureField.get(targetFeature);
        String value = null;
        // special case for covering text
        if ("*".equals(targetFeature)) {
          value = doc.getText().substring((int) current.getStart(),
                  (int) current.getEnd());
        }
        // get the value for the feature
        else {
          value = current.getFeatures().get(targetFeature);
        }
        LOG.debug("Adding field : " + SOLRFieldName + "\t" + value);
        // skip if no value has been found
        if (value != null)
          inputDoc.setField(SOLRFieldName, value);
      }
    }

    float boost = 1.0f;
    inputDoc.setDocumentBoost(boost);
    return inputDoc;
  }

  public void close() throws IOException {
    try {
      solr.commit(false, false);
    } catch (final SolrServerException e) {
      throw makeIOException(e);
    }
  }

  public static IOException makeIOException(SolrServerException e) {
    final IOException ioe = new IOException();
    ioe.initCause(e);
    return ioe;
  }

}
