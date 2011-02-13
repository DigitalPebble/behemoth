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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import com.digitalpebble.behemoth.Annotation;

/*******************************************************************************
 * SAX Handler which gets events from the Tika parser events and create Behemoth
 * annotations accordingly.
 * 
 ******************************************************************************/

public class TikaMarkupHandler implements ContentHandler {

    private StringBuffer textBuffer;

    private List<Annotation> annotationBuffer;

    private LinkedList<Annotation> startedAnnotations;

    public TikaMarkupHandler() {
        textBuffer = new StringBuffer();
        annotationBuffer = new LinkedList<Annotation>();
        startedAnnotations = new LinkedList<Annotation>();
    }

    public void characters(char[] ch, int start, int length)
            throws SAXException {
        // store the characters in the textBuffer
        textBuffer.append(ch, start, length);
    }

    public void startDocument() throws SAXException {
        textBuffer = new StringBuffer();
        annotationBuffer = new LinkedList<Annotation>();
        startedAnnotations = new LinkedList<Annotation>();
    }

    public void endDocument() throws SAXException {
        // there should be no annotation left at this stage
        if (startedAnnotations.size() != 0) {
            // TODO log + error message
        }
    }

    public void startElement(String uri, String localName, String qName,
            Attributes atts) throws SAXException {
        int startOffset = textBuffer.length();

        Annotation annot = new Annotation();
        annot.setStart(startOffset);
        // use the localname as a type
        annot.setType(localName);
        // convert the attributes into features
        for (int i = 0; i < atts.getLength(); i++) {
            String key = atts.getLocalName(i);
            String value = atts.getValue(i);
            annot.getFeatures().put(key, value);
        }
        this.startedAnnotations.addLast(annot);
    }

    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        int endOffset = textBuffer.length();

        // try to get the corresponding annotation
        // we start from the last temporary
        // and go up the stack
        Iterator<Annotation> iter = startedAnnotations.iterator();
        Annotation startedAnnot = null;
        while (iter.hasNext()) {
            Annotation temp = iter.next();
            if (temp.getType().equals(localName)) {
                startedAnnot = temp;
                break;
            }
        }
        // found something?
        if (startedAnnot == null) {
            // TODO log etc...
            return;
        }

        startedAnnot.setEnd(endOffset);
        startedAnnotations.remove(startedAnnot);
        annotationBuffer.add(startedAnnot);
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

    public List<Annotation> getAnnotations() {
        return this.annotationBuffer;
    }

}
