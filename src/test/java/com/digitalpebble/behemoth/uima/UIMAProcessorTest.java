package com.digitalpebble.behemoth.uima;

import java.io.File;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

public class UIMAProcessorTest extends TestCase {

    UIMAProcessor uima = null;

    public void setUp() throws Exception {
        // load the resources from the test environment
        String testDataDir = System.getProperty("test.build.data");
        File appDescriptor = new File(testDataDir, "WhitespaceTokenizer.pear");
        // try loading the UIMA processor
        Configuration conf = BehemothConfiguration.create();
        conf.set("uima.pear.path", appDescriptor.getCanonicalPath());
        conf.set("uima.annotations.filter", "org.apache.uima.TokenAnnotation");
        uima = new UIMAProcessor(appDescriptor.toURI().toURL());
        uima.setConf(conf);
    }

    public void tearDown() throws Exception {
        // close the GATE Processor
        uima.close();
    }

    public void testTokenizationUIMA() {
        // Create a very simple Behemoth document
        String text = "This is a simple test";
        String url = "dummyURL";
        BehemothDocument doc = new BehemothDocument();
        doc.setContent(text.getBytes());
        doc.setUrl(url);
        doc.setContentType("text/plain");
        // don't set the text as such
        // or any metadata at all
        BehemothDocument[] outputs = uima.process(doc, null);
        // the output should contain only one document
        assertEquals(1, outputs.length);
        BehemothDocument output = outputs[0];
        // the output document must have 5 annotations of type token
        // see gate.annotations.filter in Configuration above
        assertEquals(5, output.getAnnotations().size());
    }

}
