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

package com.digitalpebble.behemoth.tika;

import java.util.ArrayList;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import com.digitalpebble.behemoth.Annotation;

public class TikaTextHandler implements BehemothHandler {

    private StringBuffer textBuffer;

    public TikaTextHandler() {
        textBuffer = new StringBuffer();
    }

    public void characters(char[] ch, int start, int length)
            throws SAXException {
        textBuffer.append(ch, start, length);
    }

    public void startDocument() throws SAXException {
        textBuffer = new StringBuffer();

    }

    public void endDocument() throws SAXException {
    }

    public void startElement(String uri, String localName, String qName,
            Attributes atts) throws SAXException {
    }

    public void endElement(String uri, String localName, String qName)
            throws SAXException {
    }

    public void ignorableWhitespace(char[] ch, int start, int length)
            throws SAXException {
        characters(ch, start, length);
    }

    // the following methods are simply ignored

    public void startPrefixMapping(String prefix, String uri)
            throws SAXException {
    }

    public void endPrefixMapping(String prefix) throws SAXException {
    }

    public void setDocumentLocator(Locator locator) {
    }

    public void skippedEntity(String name) throws SAXException {
    }

    public void processingInstruction(String target, String data)
            throws SAXException {
    }

    public String getText() {
        return this.textBuffer.toString();
    }

    @Override
    public List<Annotation> getAnnotations() {
        return new ArrayList<Annotation>();
    }

}
