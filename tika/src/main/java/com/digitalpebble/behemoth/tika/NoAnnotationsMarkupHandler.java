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

import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * ****************************************************************************
 * SAX Handler which gets events from the Tika parser events and create Behemoth
 * documents, but skips the annotations
 * <p/>
 * ****************************************************************************
 */

public class NoAnnotationsMarkupHandler extends TikaMarkupHandler implements ContentHandler {


  public NoAnnotationsMarkupHandler() {
    super();
  }

  public void startDocument() throws SAXException {
    textBuffer.setLength(0);
  }

  @Override
  public void endDocument() throws SAXException {

  }


}
