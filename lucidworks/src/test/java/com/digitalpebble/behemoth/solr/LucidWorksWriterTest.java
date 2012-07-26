package com.digitalpebble.behemoth.solr;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

import com.digitalpebble.behemoth.Annotation;
import com.digitalpebble.behemoth.BehemothDocument;

public class LucidWorksWriterTest {
  private LucidWorksWriter writer;
  
  @Before
  public void setup(){
    populateSolrFieldMappingsAndCreateWriter();
  }
  
  @Test
  public void testPopulateSolrFieldMappingsFromBehemothAnnotationsTypesAndFeatures() throws Exception {
    assertEquals("person", writer.fieldMapping.get("Person").get("*"));
  }

  private void populateSolrFieldMappingsAndCreateWriter() {
    LucidWorksWriter writer = new LucidWorksWriter(null);
    JobConf job = new JobConf();
    job.setBoolean("lw.annotations", true);
    job.set("solr.f.person", "Person");
    writer.populateSolrFieldMappingsFromBehemothAnnotationsTypesAndFeatures(job);
    this.writer = writer;
  }
  
  @Test
  public void testConvertToSOLR() throws Exception {
    writer.includeAnnotations = true;
    BehemothDocument doc = new BehemothDocument();
    doc.setText("John text");
    List<Annotation> annotations = new ArrayList<Annotation>();
    Annotation annotation = new Annotation();
    annotation.setStart(0);
    annotation.setEnd(4);
    annotation.setType("Person");
    //OpenNLP Person has no features:
    //Map<String,String> features = new HashMap<String,String>();
    //features.put("string", "John");
    //annotation.setFeatures(features);
    annotations.add(annotation);
    doc.setAnnotations(annotations);
    SolrInputDocument solrDoc = writer.convertToSOLR(doc);
    assertEquals("John", solrDoc.getFieldValue("person"));
  }
}
