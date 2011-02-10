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

package com.digitalpebble.behemoth.gate;

import gate.AnnotationSet;
import gate.Corpus;
import gate.CorpusController;
import gate.Factory;
import gate.Gate;
import gate.util.OffsetComparator;
import gate.util.persistence.PersistenceManager;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.utils.ParseUtils;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;

/**
 * Encapsulates a GATE application
 **/
public class GATEProcessor implements DocumentProcessor {

    private static final Logger LOG = LoggerFactory
            .getLogger(GATEProcessor.class);

    private static boolean inited = false;

    private CorpusController GATEapplication = null;

    private Corpus corpus = null;

    private Configuration config;

    private GATEAnnotationFilters filters = null;

    private URL applicationDescriptorPath = null;

    public GATEProcessor(URL appliPath) {
        applicationDescriptorPath = appliPath;
    }

    // Process an input document with GATE
    public synchronized BehemothDocument[] process(BehemothDocument inputDoc) {
        return process(inputDoc, null);
    }

    // Process an input document with GATE and a Reporter
    public synchronized BehemothDocument[] process(BehemothDocument inputDoc,
            Reporter reporter) {
        if (reporter != null)
            reporter.setStatus("GATE : " + inputDoc.getUrl().toString());

        boolean clearAS = config.getBoolean("gate.emptyannotationset", false);
        if (clearAS)
            inputDoc.getMetadata().clear();

        // process the text passed as value with the application
        // a) create a GATE document based on the text value
        gate.Document gatedocument = null;
        try {

            // does the input document have a some text?
            // if not use Tika to extract it
            if (inputDoc.getText() == null) {
                // convert binary content into Gate doc
                InputStream is = new ByteArrayInputStream(inputDoc.getContent());
                String textContent = ParseUtils.getStringContent(is, TikaConfig
                        .getDefaultConfig(), inputDoc.getContentType());
                inputDoc.setText(textContent);
            }

            // simple version = create a GATE doc from a simple text
            // representation
            // regardless of the original markup
            gatedocument = Factory.newDocument(inputDoc.getText());

            corpus.add(gatedocument);
            // get the application and assign the corpus to it
            this.GATEapplication.setCorpus(corpus);
            // process it with GATE
            this.GATEapplication.execute();

            AnnotationSet annots = null;
            if ("".equals(filters.getAnnotationSetName()))
                annots = gatedocument.getAnnotations();
            else
                annots = gatedocument.getAnnotations(filters
                        .getAnnotationSetName());

            // enrich the input doc with the annotations from
            // the GATE application
            List<com.digitalpebble.behemoth.Annotation> beheannotations = convertGATEAnnotationsToBehemoth(
                    annots, inputDoc);

            // sort the annotations before adding them?
            Collections.sort(beheannotations);

            inputDoc.getAnnotations().addAll(beheannotations);

            // add counters about num of annotations added
            if (reporter != null)
                for (com.digitalpebble.behemoth.Annotation annot : beheannotations) {
                    reporter.incrCounter("Gate", annot.getType(), 1);
                }

            // transfer the annotations from the GATE document
            // to the Behemoth one using the filters
            if (reporter != null)
                reporter.incrCounter("GATE", "Document", 1);

        } catch (Exception e) {
            LOG.error(inputDoc.getUrl().toString(), e);
            if (reporter != null)
                reporter.incrCounter("GATE", "Exceptions", 1);
        } finally {
            // remove the document from the corpus again
            corpus.clear();
            // and from memory
            if (gatedocument != null)
                Factory.deleteResource(gatedocument);
        }
        // currently returns only the input document
        return new BehemothDocument[] { inputDoc };
    }

    public void setConf(Configuration conf) {
        config = conf;

        if (applicationDescriptorPath == null)
            throw new RuntimeException("GATE application path is null");

        // create one instance of the GATE application
        // need to avoid concurrent access to the application
        try {

            if (inited == false) {
                File gateHome = new File(applicationDescriptorPath.getFile())
                        .getParentFile();
                LOG.info("Setting GATE_HOME as " + gateHome);
                File pluginsHome = new File(gateHome, "plugins");
                // the config files are in the job archive - not in the GATE
                // application
                // zip
                // File siteConfigFile = new File(conf
                // .getResource("site-gate.xml").getFile());
                // File userConfig = new File(conf.getResource("user-gate.xml")
                // .getFile());
                Gate.runInSandbox(true);
                Gate.setGateHome(gateHome);
                Gate.setPluginsHome(pluginsHome);
                // Gate.setSiteConfigFile(siteConfigFile);
                // Gate.setUserConfigFile(userConfig);
                // the builtInCreoleDir files
                // are stored in the same place as the config ones
                // Gate.setBuiltinCreoleDir(conf.getResource("creole.xml"));
                Gate.init();
                inited = true;
            }

            corpus = Factory.newCorpus("DummyCorpus");

            this.GATEapplication = (CorpusController) PersistenceManager
                    .loadObjectFromUrl(applicationDescriptorPath);

            // load the annotation and feature filters from the configuration
            this.filters = GATEAnnotationFilters.getFilters(config);
        } catch (Exception e) {
            LOG.error("Encountered error while initialising GATE", e);
            throw new RuntimeException(e);
        }
    }

    public void close() {
        if (GATEapplication != null)
            Factory.deleteResource(GATEapplication);
    }

    public Configuration getConf() {
        return config;
    }

    /**
     * Returns a list of annotations to be added to the Behemoth document from
     * the GATE one
     ***/
    private List<com.digitalpebble.behemoth.Annotation> convertGATEAnnotationsToBehemoth(
            AnnotationSet GATEAnnotionSet,
            com.digitalpebble.behemoth.BehemothDocument behemoth) {

        List<com.digitalpebble.behemoth.Annotation> beheannotations = new ArrayList<com.digitalpebble.behemoth.Annotation>();

        AnnotationSet resultAS = GATEAnnotionSet.get(filters.getTypes());

        // sort the GATE annotations
        List<gate.Annotation> annotationList = new ArrayList<gate.Annotation>(
                resultAS);
        Collections.sort(annotationList, new OffsetComparator());
        Iterator<gate.Annotation> inputASIter = annotationList.iterator();

        while (inputASIter.hasNext()) {
            gate.Annotation source = inputASIter.next();

            com.digitalpebble.behemoth.Annotation target = new com.digitalpebble.behemoth.Annotation();
            target.setType(source.getType());
            target.setStart(source.getStartNode().getOffset().longValue());
            target.setEnd(source.getEndNode().getOffset().longValue());

            // now do the features
            // is the type listed?
            Set<String> expectedfeatnames = filters.getFeatfilts().get(
                    source.getType());
            if (expectedfeatnames != null) {
                Iterator featurenames = source.getFeatures().keySet()
                        .iterator();
                while (featurenames.hasNext()) {
                    // cast the feature name to a string which will be right in
                    // 99% of cases
                    String featurename = featurenames.next().toString();
                    // if this feature name is not wanted just ignore it
                    if (expectedfeatnames.contains(featurename) == false)
                        continue;
                    // we know that we want to keep this feature
                    // let's see what the best way of representing the value
                    // would be
                    // TODO later => find a better way of mapping when not a
                    // string
                    Object originalvalue = source.getFeatures()
                            .get(featurename);
                    if (originalvalue == null)
                        originalvalue = "null";
                    target.getFeatures().put(featurename,
                            originalvalue.toString());
                }
            }
            beheannotations.add(target);
        }
        return beheannotations;
    }

}
