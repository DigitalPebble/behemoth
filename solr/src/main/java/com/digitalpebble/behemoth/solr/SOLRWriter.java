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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;

public class SOLRWriter {

    private static final Log LOG = LogFactory.getLog(SOLRWriter.class);

    private StreamingUpdateSolrServer solr;

    // key = Annotation type ; value = feature name / SOLR field
    Map<String, Map<String, String>> fieldMapping = new HashMap<String, Map<String, String>>();

    public void open(JobConf job, String name) throws IOException {
        String solrURL = job.get("solr.server.url");
        int queueSize = job.getInt("solr.client.queue.size", 100);
        int threadCount = job.getInt("solr.client.threads", 1);
        solr = new StreamingUpdateSolrServer(solrURL, queueSize, threadCount);
        /* Generate mapping for Behemoth annotations/features to Solr fields
         * config values look like solr.f.<solr field> = <annotation type>.<feature>
         * E.g.,
         *   solr.f.foo = bar
         *   solr.f.foo = spam.eggs
         * generates the mapping {"bar":{"*","foo"}, "spam":{"eggs":"foo"}}
         */
        Iterator<Entry<String, String>> iterator = job.iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            if (entry.getKey().startsWith("solr.f.") == false)
                continue;
            String solrFieldName = entry.getKey().substring("solr.f.".length());

            // Split the annotation type and feature name (e.g., Person.string)
            String[] toks = entry.getValue().split("\\.");
            String annotationName = null;
            String featureName = null;
            if(toks.length == 1) {
              annotationName = toks[0];
            } else if(toks.length == 2) {
              annotationName = toks[0];
              featureName = toks[1];
            } else {
              LOG.warn("Invalid annotation field mapping: " + entry.getValue());
            }

            Map<String, String> featureMap = fieldMapping.get(annotationName);
            if(featureMap == null) {
              featureMap = new HashMap<String, String>();
            }

            // If not feature name is given (e.g., Person instead of Person.string), infer a *
            if(featureName == null)
              featureName = "*";

            featureMap.put(featureName, solrFieldName);
            fieldMapping.put(annotationName, featureMap);

            LOG.debug("Adding mapping for annotation " + annotationName +
                     ", feature '" + featureName + "' to  Solr field '" + solrFieldName + "'");
        }
    }

    public void write(BehemothDocument doc) throws IOException {
        final SolrInputDocument inputDoc = convertToSOLR(doc);
        try {
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
        LOG.debug("Adding field id: " + doc.getUrl());

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
                    inputDoc.addField(SOLRFieldName, value);
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
