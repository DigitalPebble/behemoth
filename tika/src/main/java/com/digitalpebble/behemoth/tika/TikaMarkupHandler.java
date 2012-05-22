package com.digitalpebble.behemoth.tika;
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

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

/**
 *
 *
 **/
public abstract class TikaMarkupHandler implements ContentHandler {

  protected StringBuilder textBuffer;

  public TikaMarkupHandler() {
    textBuffer = new StringBuilder();
  }

  @Override
  public void startElement(String s, String s1, String s2, Attributes attributes) throws SAXException {

  }

  public void endElement(String uri, String localName, String qName)
          throws SAXException {

    int endOffset = textBuffer.length();

    // add a \n after the head if the text is not empty
    // i.e. there is a title
    if (localName.equals("head") && endOffset > 0)
      textBuffer.append("\n");
  }

  public String getText() {
    return this.textBuffer.toString();
  }

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

  public void ignorableWhitespace(char[] ch, int start, int length)
          throws SAXException {
    characters(ch, start, length);
  }

  public void characters(char[] ch, int start, int length)
          throws SAXException {
    textBuffer.append(ch, start, length);
  }
}
