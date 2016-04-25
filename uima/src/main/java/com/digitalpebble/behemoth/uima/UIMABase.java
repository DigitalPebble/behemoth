package com.digitalpebble.behemoth.uima;

import com.digitalpebble.behemoth.BehemothDocument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reporter;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.pear.tools.PackageBrowser;
import org.apache.uima.resource.ResourceManager;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.XMLInputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by georgekankava on 4/25/16.
 */
public class UIMABase extends MapReduceBase {

    private static final Logger LOG = LoggerFactory
            .getLogger(UIMABase.class);

    protected AnalysisEngine tae;

    protected CAS cas;

    protected Configuration config;

    protected boolean storeshortnames = true;

    protected Map<String, Set<Feature>> featfilts = new HashMap<String, Set<Feature>>();


    protected void doProcess(BehemothDocument behemoth, Reporter reporter) throws AnalysisEngineProcessException {
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
            convertCASToBehemoth(cas, behemoth, reporter);
        }
    }

    /** convert the annotations from the CAS into the Behemoth format **/
    protected void convertCASToBehemoth(CAS uimadoc,
                                        com.digitalpebble.behemoth.BehemothDocument behemoth,
                                        Reporter reporter) {

        String[] annotTypes = config.get("uima.annotations.filter", "").split(
                ",");
        List<Type> uimatypes = new ArrayList<Type>(annotTypes.length);

        for (String type : annotTypes) {
            Type aType = cas.getTypeSystem().getType(type);
            uimatypes.add(aType);
        }

        FSIterator<AnnotationFS> annotIterator = cas.getAnnotationIndex()
                .iterator();
        while (annotIterator.hasNext()) {
            AnnotationFS annotation = annotIterator.next();
            if (!uimatypes.contains(annotation.getType()))
                continue;
            String atype = annotation.getType().toString();
            // wanted type -> generate an annotation for it
            if (reporter != null)
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

    protected void newCAS(PackageBrowser instPear, ResourceManager rsrcMgr) {
        // Create analysis engine from the installed PEAR package using
        // the created PEAR specifier
        XMLInputSource in = null;
        try {
            in = new XMLInputSource(instPear.getComponentPearDescPath());
            ResourceSpecifier specifier = UIMAFramework.getXMLParser().parseResourceSpecifier(in);
            tae = UIMAFramework.produceAnalysisEngine(specifier, rsrcMgr, null);
            cas = tae.newCAS();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if(in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    LOG.error("Exception while trying to close resource", e);
                }
            }
        }
    }

    protected void fillFeatFilts() {
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
    }
}
