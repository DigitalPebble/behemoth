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

package com.digitalpebble.behemoth.uima;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.AnnotationImpl;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.pear.tools.PackageBrowser;
import org.apache.uima.pear.tools.PackageInstaller;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.XMLInputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;

public class UIMAMapper extends 
        Mapper<Text, BehemothDocument, Text, BehemothDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(UIMAMapper.class);

    private AnalysisEngine tae;

    private CAS cas;

    private Configuration config;

    private boolean storeshortnames = true;

    private List<Type> uimatypes = new ArrayList<Type>();

    private Map<String, Set<Feature>> featfilts = new HashMap<String, Set<Feature>>();

    @Override
    public void map(Text id, BehemothDocument behemoth,
            Mapper<Text, BehemothDocument, Text, BehemothDocument>.Context context)
            throws IOException, InterruptedException {

        context.setStatus("UIMA : " + id.toString());

        // generate a CAS from the input document
        cas.reset();

        try {
            // does the input document have a some text?
            // if not - skip it
            if (behemoth.getText() == null) {
                LOG.debug(behemoth.getUrl().toString() + " has null text");
            } else {
                // detect language if specified by user
                String lang = this.config.get("uima.language", "en");
                cas.setDocumentLanguage(lang);
                cas.setDocumentText(behemoth.getText());
                // process it
                tae.process(cas);
                convertCASToBehemoth(cas, behemoth, context);
            }
        } catch (Exception e) {
            context.getCounter("UIMA", "Exception").increment(1);
            throw new IOException(e);
        }

        context.getCounter("UIMA", "Document").increment(1);

        // dump the modified document
        context.write(id, behemoth);
    }

    @Override        
    public void setup(Context context) {

        this.config = context.getConfiguration();

        storeshortnames = config.getBoolean("uima.store.short.names", true);
        
        File pearpath = new File(config.get("uima.pear.path"));
        String pearname = pearpath.getName();

        URL urlPEAR = null;

        try {
            Path[] localArchives = DistributedCache.getLocalCacheFiles(config);
            // identify the right archive
            for (Path la : localArchives) {
                String localPath = la.toUri().toString();
                LOG.info("Inspecting local paths " + localPath);
                if (!localPath.endsWith(pearname))
                    continue;
                urlPEAR = new URL("file://" + localPath);
                break;
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Impossible to retrieve gate application from distributed cache",
                    e);
        }

        if (urlPEAR == null)
            throw new RuntimeException("UIMA pear " + pearpath
                    + " not available in distributed cache");

        File pearFile = new File(urlPEAR.getPath());

        File installPath = new File("/tmp/uima/");
        PackageBrowser instPear = PackageInstaller.installPackage(installPath,
                    pearFile, true);

        // get the resources required for the AnalysisEngine
        org.apache.uima.resource.ResourceManager rsrcMgr = UIMAFramework
                .newDefaultResourceManager();

        // Create analysis engine from the installed PEAR package using
        // the created PEAR specifier
        XMLInputSource in;
        try {
            in = new XMLInputSource(instPear.getComponentPearDescPath());

            ResourceSpecifier specifier = UIMAFramework.getXMLParser()
                    .parseResourceSpecifier(in);

            tae = UIMAFramework.produceAnalysisEngine(specifier, rsrcMgr, null);

            cas = tae.newCAS();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String[] featuresFilters = this.config.get("uima.features.filter", "")
                .split(",");
        // the featurefilters have the following form : Type:featureName
        // we group them by annotation type
        for (String ff : featuresFilters) {
            String[] fp = ff.split(":");
            if (fp.length != 2)
                continue;
            Set<Feature> features = featfilts.get(fp[0]);
            if (features == null) {
                features = new HashSet<Feature>();
                featfilts.put(fp[0], features);
            }
            Feature f = cas.getTypeSystem().getFeatureByFullName(ff);
            if (f != null)
                features.add(f);
        }

        String[] annotTypes = this.config.get("uima.annotations.filter", "")
                .split(",");
        uimatypes = new ArrayList<Type>(annotTypes.length);

        for (String type : annotTypes) {
            Type aType = cas.getTypeSystem().getType(type);
            uimatypes.add(aType);
        }

    }

    @Override
    public void cleanup(Context context) throws IOException {
        if (cas != null)
            cas.release();
        if (tae != null)
            tae.destroy();
    }

    /** convert the annotations from the CAS into the Behemoth format **/
    private void convertCASToBehemoth(CAS uimadoc,
            com.digitalpebble.behemoth.BehemothDocument behemoth,
            Mapper<Text, BehemothDocument, Text, BehemothDocument>.Context reporter) {

        String[] annotTypes = config.get("uima.annotations.filter", "").split(
                ",");
        List<Type> uimatypes = new ArrayList<Type>(annotTypes.length);

        for (String type : annotTypes) {
            Type aType = cas.getTypeSystem().getType(type);
            uimatypes.add(aType);
        }

        FSIterator<AnnotationFS> annotIterator = cas.getAnnotationIndex().iterator();

        while (annotIterator.hasNext()) {
            Object annotObject = annotIterator.next();
            if (annotObject instanceof AnnotationImpl == false)
                continue;
            AnnotationImpl annotation = (AnnotationImpl) annotObject;
            if (!uimatypes.contains(annotation.getType()))
                continue;
            String atype = annotation.getType().toString();
            // wanted type -> generate an annotation for it
            reporter.getCounter("UIMA", atype).increment(1);

            com.digitalpebble.behemoth.Annotation target = new com.digitalpebble.behemoth.Annotation();
            // short version?
            if (storeshortnames)
                target.setType(annotation.getType().getShortName());
            else
                target.setType(atype);
            target.setStart(annotation.getBegin());
            target.setEnd(annotation.getEnd());
            // now get the features for this annotation
            Set<Feature> possiblefeatures = featfilts.get(atype);
            if (possiblefeatures != null) {
                // iterate on the expected features
                Iterator<Feature> possiblefeaturesIterator = possiblefeatures
                        .iterator();
                while (possiblefeaturesIterator.hasNext()) {
                    Feature expectedFeature = possiblefeaturesIterator.next();
                    String fvalue = annotation
                            .getFeatureValueAsString(expectedFeature);
                    if (fvalue == null)
                        fvalue = "null";
                    // always use the short names for the features
                    target.getFeatures().put(expectedFeature.getShortName(),
                            fvalue);
                }
            }
            // add current annotation to Behemoth document
            behemoth.getAnnotations().add(target);
        }

    }
}
