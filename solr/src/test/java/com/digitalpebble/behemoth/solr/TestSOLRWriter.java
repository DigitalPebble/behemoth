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

        assertEquals(writer.getFieldMapping().size(), 2);
        assertNotNull(writer.getFieldMapping().get("Person"));
        assertEquals(writer.getFieldMapping().get("Person").size(), 2);
        assertEquals(writer.getFieldMapping().get("Person").get("string"),
                "person");
        assertEquals(writer.getFieldMapping().get("Person").get("title"),
                "personTitle");
        assertNotNull(writer.getFieldMapping().get("Location"));
        assertEquals(writer.getFieldMapping().get("Location").size(), 1);
        assertEquals(writer.getFieldMapping().get("Location").get("*"),
                "location");
    }
}