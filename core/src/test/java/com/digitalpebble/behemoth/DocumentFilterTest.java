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

package com.digitalpebble.behemoth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class DocumentFilterTest extends TestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testPositiveFilter() {
        Configuration config = BehemothConfiguration.create();
        config.set(DocumentFilter.DocumentFilterParamNamePrefixKeep + "lang",
                "en");

        DocumentFilter filter = DocumentFilter.getFilters(config);

        BehemothDocument doc = new BehemothDocument();
        doc.getMetadata(true).put(new Text("lang"), new Text("en"));
        boolean kept = filter.keep(doc);
        assertEquals(true, kept);

        doc.getMetadata(true).put(new Text("lang"), new Text("fr"));
        kept = filter.keep(doc);
        assertEquals(false, kept);

        doc.getMetadata(true).remove(new Text("lang"));
        kept = filter.keep(doc);
        assertEquals(false, kept);

        // multiple constraints
        config = BehemothConfiguration.create();
        config.set(DocumentFilter.DocumentFilterParamNamePrefixKeep + "lang",
                "en");
        config.set(DocumentFilter.DocumentFilterParamNamePrefixKeep + "tc",
                "true");
        filter = DocumentFilter.getFilters(config);

        doc.getMetadata(true).put(new Text("lang"), new Text("en"));
        doc.getMetadata(true).put(new Text("tc"), new Text("true"));
        kept = filter.keep(doc);
        assertEquals(true, kept);

        doc = new BehemothDocument();
        doc.getMetadata(true).put(new Text("lang"), new Text("fr"));
        doc.getMetadata(true).put(new Text("tc"), new Text("true"));
        kept = filter.keep(doc);
        assertEquals(false, kept);

        doc = new BehemothDocument();
        doc.getMetadata(true).put(new Text("lang"), new Text("fr"));
        doc.getMetadata(true).put(new Text("tc"), new Text("false"));
        kept = filter.keep(doc);
        assertEquals(false, kept);
    }

    public void testNegativeFilter() {
        Configuration config = BehemothConfiguration.create();
        config.set(DocumentFilter.DocumentFilterParamNamePrefixSkip + "lang",
                ".+");

        DocumentFilter filter = DocumentFilter.getFilters(config);
        BehemothDocument doc = new BehemothDocument();
        doc.getMetadata(true).put(new Text("lang"), new Text("en"));
        boolean kept = filter.keep(doc);
        assertEquals(false, kept);

        doc = new BehemothDocument();
        kept = filter.keep(doc);
        assertEquals(true, kept);

        // multiple constraints
        config = BehemothConfiguration.create();
        config.set(DocumentFilter.DocumentFilterParamNamePrefixSkip + "lang",
                "en");
        config.set(DocumentFilter.DocumentFilterParamNamePrefixSkip + "tc",
                "true");
        filter = DocumentFilter.getFilters(config);

        doc.getMetadata(true).put(new Text("lang"), new Text("en"));
        doc.getMetadata(true).put(new Text("tc"), new Text("true"));
        kept = filter.keep(doc);
        assertEquals(false, kept);

        doc = new BehemothDocument();
        doc.getMetadata(true).put(new Text("lang"), new Text("fr"));
        doc.getMetadata(true).put(new Text("tc"), new Text("true"));
        kept = filter.keep(doc);
        assertEquals(true, kept);

        doc = new BehemothDocument();
        doc.getMetadata(true).put(new Text("lang"), new Text("fr"));
        doc.getMetadata(true).put(new Text("tc"), new Text("false"));
        kept = filter.keep(doc);
        assertEquals(true, kept);

    }

    public void testURLFilter() {
        Configuration config = BehemothConfiguration.create();
        config.set(DocumentFilter.DocumentFilterParamNameURLFilterKeep, ".+");

        // no URL set - must fail
        DocumentFilter filter = DocumentFilter.getFilters(config);
        BehemothDocument doc = new BehemothDocument();
        doc.getMetadata(true).put(new Text("lang"), new Text("en"));
        boolean kept = filter.keep(doc);
        assertEquals(false, kept);

        doc = new BehemothDocument();
        doc.setUrl("any random rubbish will do");
        kept = filter.keep(doc);
        assertEquals(true, kept);
    }

    public void testURLFilterRequired() {
        Configuration config = BehemothConfiguration.create();
        // no filters set : must fail
        assertEquals(false, DocumentFilter.isRequired(config));
        config.set(DocumentFilter.DocumentFilterParamNameURLFilterKeep, ".+");
        assertEquals(true, DocumentFilter.isRequired(config));
    }

}
