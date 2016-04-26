package com.digitalpebble.behemoth.uima;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.uima.resource.ResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.pear.tools.PackageBrowser;
import org.apache.uima.pear.tools.PackageInstaller;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.XMLInputSource;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;

public class UIMAProcessor extends UIMABase implements DocumentProcessor {

    private static final Logger LOG = LoggerFactory
            .getLogger(UIMAProcessor.class);

    private URL urlPEAR = null;

    private List<Type> uimatypes = new ArrayList<Type>();

    public UIMAProcessor(URL appliPath) {
        urlPEAR = appliPath;
    }

    public void close() {
        if (cas != null)
            cas.release();
        if (tae != null)
            tae.destroy();
    }

    public BehemothDocument[] process(BehemothDocument behemoth,
            Reporter reporter) {
        if (reporter != null)
            reporter.setStatus("UIMA : " + behemoth.getUrl().toString());

        // generate a CAS from the input document
        cas.reset();

        try {
            doProcess(behemoth, reporter);
        } catch (Exception e) {
            if (reporter != null)
                reporter.incrCounter("UIMA", "Exception", 1);
            LOG.error(behemoth.getUrl().toString(), e);
        }

        if (reporter != null)
            reporter.incrCounter("UIMA", "Document", 1);

        // return the modified document
        return new BehemothDocument[] { behemoth };
    }

    public Configuration getConf() {
        return config;
    }

    public void setConf(Configuration conf) {
        long initStart = System.currentTimeMillis();
        config = conf;
        storeshortnames = config.getBoolean("uima.store.short.names", true);
        File pearFile = new File(urlPEAR.getPath());
        PackageBrowser instPear = PackageInstaller.installPackage(
                pearFile.getParentFile(), pearFile, true);

        // get the resources required for the AnalysisEngine
        org.apache.uima.resource.ResourceManager rsrcMgr = UIMAFramework
                .newDefaultResourceManager();

        newCAS(instPear, rsrcMgr);

        fillFeatFilts();

        String[] annotTypes = this.config.get("uima.annotations.filter", "")
                .split(",");
        uimatypes = new ArrayList<Type>(annotTypes.length);

        for (String type : annotTypes) {
            Type aType = cas.getTypeSystem().getType(type);
            uimatypes.add(aType);
        }
        long initEnd = System.currentTimeMillis();
        LOG.info("Initialisation of UIMAProcessor done in "
                + (initEnd - initStart) + " msec");
    }

}
