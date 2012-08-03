package com.digitalpebble.behemoth.solr;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import org.junit.Test;
import org.apache.hadoop.mapred.JobConf;

public class TestSOLRWriter {
  @Test
  public void testFieldMappings() throws IOException {
    JobConf conf = new JobConf();
    conf.set("solr.server.url", "http://example.org");
    conf.set("solr.f.person", "Person.string");
    conf.set("solr.f.personTitle", "Person.title");
    conf.set("solr.f.location", "Location");

    SOLRWriter writer = new SOLRWriter();
    writer.open(conf, "test");

    assertEquals(writer.fieldMapping.size(), 2);
    assertNotNull(writer.fieldMapping.get("Person"));
    assertEquals(writer.fieldMapping.get("Person").size(), 2);
    assertEquals(writer.fieldMapping.get("Person").get("string"), "person");
    assertEquals(writer.fieldMapping.get("Person").get("title"), "personTitle");
    assertNotNull(writer.fieldMapping.get("Location"));
    assertEquals(writer.fieldMapping.get("Location").size(), 1);
    assertEquals(writer.fieldMapping.get("Location").get("*"), "location");
  }
}
