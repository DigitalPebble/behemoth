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
 **/
package com.digitalpebble.behemoth.solr;

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class SOLRWriter {

    private static final Log LOG = LogFactory.getLog(SOLRWriter.class);

    private SolrServer solr;

    // key = Annotation type ; value = feature name / SOLR field
    protected Map<String, Map<String, String>> fieldMapping = new HashMap<String, Map<String, String>>();
    private Progressable progress;
    private boolean includeMetadata = false;
    protected boolean includeAnnotations = false;
    protected boolean includeAllAnnotations = false;
    protected boolean useMetadataPrefix = false;
    protected String metadataPrefix = null;
    protected String annotationPrefix = null;
    protected boolean useAnnotationPrefix = false;
    protected ModifiableSolrParams params = null;

    public SOLRWriter(Progressable progress) {
        this.progress = progress;
    }

    public void open(JobConf job, String name) throws IOException {
        String zkHost = job.get("solr.zkhost");
        if (zkHost != null && zkHost.equals("") == false) {
            String collection = job.get("solr.zk.collection", "collection1");
            LOG.info("Indexing to collection: " + collection + " w/ ZK host: "
                    + zkHost);
            solr = new CloudSolrServer(zkHost);
            ((CloudSolrServer) solr).setDefaultCollection(collection);
        } else {
            String solrURL = job.get("solr.server.url");
            int queueSize = job.getInt("solr.client.queue.size", 100);
            int threadCount = job.getInt("solr.client.threads", 1);
            solr = new ConcurrentUpdateSolrServer(solrURL, queueSize,
                    threadCount);
        }
        String paramsString = job.get("solr.params");
        if (paramsString != null) {
            params = new ModifiableSolrParams();
            String[] pars = paramsString.trim().split("\\&");
            for (String kvs : pars) {
                String[] kv = kvs.split("=");
                if (kv.length < 2) {
                    LOG.warn("Invalid Solr param " + kvs + ", skipping...");
                    continue;
                }
                params.add(kv[0], kv[1]);
            }
            LOG.info("Using Solr params: " + params.toString());
        }

        includeMetadata = job.getBoolean("solr.metadata", false);
        includeAnnotations = job.getBoolean("solr.annotations", false);
        useMetadataPrefix = job.getBoolean("solr.metadata.use.prefix", false);
        metadataPrefix = job.get("solr.metadata.prefix", "attr_");
        annotationPrefix = job.get("solr.annotation.prefix", "annotate_");
        useAnnotationPrefix = job.getBoolean("solr.annotation.use.prefix", false);
        populateSolrFieldMappingsFromBehemothAnnotationsTypesAndFeatures(job);
    }

    protected void populateSolrFieldMappingsFromBehemothAnnotationsTypesAndFeatures(
            JobConf job) {
        // get the Behemoth annotations types and features
        // to store as SOLR fields
        // solr.f.name = BehemothType.featureName
        // e.g. solr.f.person = Person.string will map the "string" feature of
        // "Person" annotations onto the Solr field "person"
        Iterator<Entry<String, String>> iterator = job.iterator();
        while (iterator.hasNext()) {
            Entry<String, String> entry = iterator.next();
            if (entry.getKey().startsWith("solr.f.") == false)
                continue;
            String solrFieldName = entry.getKey().substring("solr.f.".length());
            populateMapping(solrFieldName, entry.getValue());
        }
        
        String list = job.get("solr.annotations.list");
        if (useAnnotationPrefix){
          if ( list == null || list.trim().length() == 0) 
            // Include all annotations if no annotations list is not defined
            includeAllAnnotations = true;
          else {
            // Include only annotations defined in the "solr.annotations.list" with the prefix
            String[] names = list.split("\\s+");
            for (String name : names){
              String solrFieldName = annotationPrefix + name;
              populateMapping(solrFieldName, name);
            }
          }
        } else {
         // Include specified annotations without prefix if annotations list is defined. 
         // These fields would have to explicitly defined in Solr schema since solr.annotation.use.prefix 
         // is not defined or field mapping has to be defined
            if (list == null || list.trim().length() == 0) {
              return;
            }
            String[] names = list.split("\\s+");
            for (String name : names) {
                String solrFieldName = name;
                populateMapping(solrFieldName, name);
            }
        }

    }

    private void populateMapping(String solrFieldName, String value) {
        // see if a feature has been specified
        // if not we'll use '*' to indicate that we want
        // the text covered by the annotation
        // HashMap<String, String> featureValMap = new HashMap<String,
        // String>();

        String[] toks = value.split("\\.");
        String annotationName = null;
        String featureName = null;
        if (toks.length == 1) {
            annotationName = toks[0];
        } else if (toks.length == 2) {
            annotationName = toks[0];
            featureName = toks[1];
        } else {
            LOG.warn("Invalid annotation field mapping: " + value);
        }

        Map<String, String> featureMap = fieldMapping.get(annotationName);
        if (featureMap == null) {
            featureMap = new HashMap<String, String>();
        }

        if (featureName == null)
            featureName = "*";

        featureMap.put(featureName, solrFieldName);
        fieldMapping.put(annotationName, featureMap);
        LOG.info("Adding mapping for annotation " + annotationName
                + ", feature '" + featureName + "' to  Solr field '"
                + solrFieldName + "'");
    }

    public void write(BehemothDocument doc) throws IOException {
        final SolrInputDocument inputDoc = convertToSOLR(doc);
        try {
            progress.progress();
            if (params == null) {
                solr.add(inputDoc);
            } else {
                UpdateRequest req = new UpdateRequest();
                req.setParams(params);
                req.add(inputDoc);
                solr.request(req);
            }
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

        LOG.debug("Adding field : id\t" + doc.getUrl());

        // Rely on the field mapping to handle this, or the dynamic
        // fields
        MapWritable metadata = doc.getMetadata();
        if (includeMetadata && metadata != null) {
            for (Entry<Writable, Writable> entry : metadata.entrySet()) {
              if (useMetadataPrefix) {
                String key = metadataPrefix + entry.getKey().toString();
                inputDoc.addField(key, entry.getValue().toString());
              }
              else {
                inputDoc.addField(entry.getKey().toString(), entry.getValue().toString()); 
              }
            }
        }
        // iterate on the annotations of interest and
        // create a new field for each one
        // it is advised NOT to set frequent annotation types
        // such as token as this would generate a stupidly large
        // number of fields which won't be used by SOLR for
        // tokenizing anyway.
        // what you can do though is to concatenate the token values
        // to form a new content string separated by spaces

        // iterate on the annotations
        if (includeAnnotations) {
            Iterator<Annotation> iterator = doc.getAnnotations().iterator();
            while (iterator.hasNext()) {
                Annotation current = iterator.next();
                // check whether it belongs to a type we'd like to send to SOLR
                Map<String, String> featureField = fieldMapping.get(current
                        .getType());
                // special case of all annotations
                if (featureField == null && !includeAllAnnotations) {
                    continue;
                }
                if (!includeAllAnnotations) {
                    // iterate on the expected features
                    for (String targetFeature : featureField.keySet()) {
                        String SOLRFieldName = featureField.get(targetFeature);
                        String value = null;
                        // special case for covering text
                        if ("*".equals(targetFeature)) {
                            value = doc.getText().substring(
                                    (int) current.getStart(),
                                    (int) current.getEnd());
                        }
                        // get the value for the feature
                        else {
                            value = current.getFeatures().get(targetFeature);
                        }
                        LOG.debug("Adding field : " + SOLRFieldName + "\t"
                                + value);
                        // skip if no value has been found
                        if (value != null)
                            inputDoc.addField(SOLRFieldName, value);
                    }
                } else {
                    for (Entry<String, String> e : current.getFeatures()
                            .entrySet()) {
                        inputDoc.addField(annotationPrefix + current.getType()
                                + "." + e.getKey(), e.getValue());
                    }
                }
            }
        }

        float boost = 1.0f;
        inputDoc.setDocumentBoost(boost);
        return inputDoc;
    }

    public void close() throws IOException {
        try {
            solr.commit(false, false);
            solr.shutdown();
        } catch (final SolrServerException e) {
            throw makeIOException(e);
        }
    }

    public static IOException makeIOException(SolrServerException e) {
        final IOException ioe = new IOException();
        ioe.initCause(e);
        return ioe;
    }

    public Map<String, Map<String, String>> getFieldMapping() {
        return fieldMapping;
    }

}
