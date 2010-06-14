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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.utils.ParseUtils;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.AnnotationImpl;
import org.apache.uima.pear.tools.PackageBrowser;
import org.apache.uima.pear.tools.PackageInstaller;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.XMLInputSource;

import com.digitalpebble.behemoth.BehemothDocument;

public class UIMAMapper extends MapReduceBase implements
	Mapper<Text, BehemothDocument, Text, BehemothDocument> {

    private static final Log LOG = LogFactory.getLog(UIMAMapper.class);

    private AnalysisEngine tae;

    private CAS cas;

    private Configuration config;

    private boolean storeshortnames = true;

    private List<Type> uimatypes = new ArrayList<Type>();

    private Map<String, Set<Feature>> featfilts = new HashMap<String, Set<Feature>>();

    @Override
    public void map(Text id, BehemothDocument behemoth,
	    OutputCollector<Text, BehemothDocument> output, Reporter reporter)
	    throws IOException {

	reporter.setStatus("UIMA : " + id.toString());

	// generate a CAS from the input document
	cas.reset();

	try {
	    // does the input document have a some text?
	    // if not use Tika to extract it
	    if (behemoth.getText() == null) {
		// convert binary content into Gate doc
		InputStream is = new ByteArrayInputStream(behemoth.getContent());
		String textContent = ParseUtils.getStringContent(is, TikaConfig
			.getDefaultConfig(), behemoth.getContentType());
		behemoth.setText(textContent);
	    }
	    // detect language if specified by user
	    String lang = this.config.get("uima.language", "en");
	    cas.setDocumentLanguage(lang);

	    cas.setDocumentText(behemoth.getText());
	    // process it
	    tae.process(cas);
	    convertCASToBehemoth(cas, behemoth, reporter);
	} catch (Exception e) {
	    reporter.incrCounter("UIMA", "Exception", 1);
	    throw new IOException(e);
	}

	reporter.incrCounter("UIMA", "Document", 1);

	// dump the modified document
	output.collect(id, behemoth);
    }

    public void configure(JobConf conf) {

	this.config = conf;

	storeshortnames = config.getBoolean("uima.store.short.names", true);

	File pearpath = new File(conf.get("uima.pear.path"));
	String pearname = pearpath.getName();

	URL urlPEAR = null;

	try {
	    Path[] localArchives = DistributedCache.getLocalCacheFiles(conf);
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

	PackageBrowser instPear = PackageInstaller.installPackage(pearFile
		.getParentFile(), pearFile, true);

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

    public void close() throws IOException {
	if (cas != null)
	    cas.release();
	if (tae != null)
	    tae.destroy();
    }

    /** convert the annotations from the CAS into the Behemoth format **/
    private void convertCASToBehemoth(CAS uimadoc,
	    com.digitalpebble.behemoth.BehemothDocument behemoth,
	    Reporter reporter) {

	String[] annotTypes = config.get("uima.annotations.filter", "").split(
		",");
	List<Type> uimatypes = new ArrayList<Type>(annotTypes.length);

	for (String type : annotTypes) {
	    Type aType = cas.getTypeSystem().getType(type);
	    uimatypes.add(aType);
	}

	FSIterator annotIterator = cas.getAnnotationIndex().iterator();

	while (annotIterator.hasNext()) {
	    Object annotObject = annotIterator.next();
	    if (annotObject instanceof AnnotationImpl == false)
		continue;
	    AnnotationImpl annotation = (AnnotationImpl) annotObject;
	    if (!uimatypes.contains(annotation.getType()))
		continue;
	    String atype = annotation.getType().toString();
	    // wanted type -> generate an annotation for it
	    reporter.incrCounter("UIMA", atype, 1);

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
