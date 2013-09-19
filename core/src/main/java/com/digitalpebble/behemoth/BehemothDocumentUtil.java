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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** 
 * Collection of utility methods on top of an AVRO-generated BehemothDocument
 ***/
public class BehemothDocumentUtil {

    public static Map<String, String> getOrCreateMetadata(BehemothDocument doc) {
        Map<String, String> md = doc.getMetadata();
        if (md != null)
            return doc.getMetadata();
        md = new HashMap<String, String>();
        doc.setMetadata(md);
        return md;
    }

    public static List<Annotation> getOrCreateAnnotations(BehemothDocument doc) {
        List<Annotation> annots = doc.getAnnotations();
        if (annots != null)
            return doc.getAnnotations();
        annots = new ArrayList<Annotation>();
        doc.setAnnotations(annots);
        return annots;
    }

    public static Map<String, String> getOrCreateFeatures(Annotation annot) {
        Map<String, String> features = annot.getFeatures();
        if (features != null)
            return features;
        features = new HashMap<String, String>();
        annot.setFeatures(features);
        return features;
    }
    
    public static byte[] getContentAsByteArray(BehemothDocument doc) {
        if (doc.getContent() == null)
            return null;
        byte[] b = new byte[doc.getContent().remaining()];
        doc.getContent().get(b, 0, b.length);
        return b;
    }

    /**
     * Returns a complete string representation of the document
     **/
    public static String toString(BehemothDocument doc) {
        return toString(doc, true, true, true, true);
    }

    /**
     * Returns a string representation of the document
     * 
     * @param binaryContent
     *            whether to include the binary content
     **/
    public static String toString(BehemothDocument doc, boolean binaryContent) {
        return toString(doc, binaryContent, true, true, true);
    }

    /**
     * Returns a string representation of the document
     * 
     * @param showContent
     *            whether to include the binary content
     * @param showAnnotations
     *            whether to include the annotations content
     * @param showText
     *            whether to include the text
     * @param showMD
     *            whether to include the metadata
     * @return
     **/
    public static String toString(BehemothDocument doc, boolean showContent,
            boolean showAnnotations, boolean showText, boolean showMD) {
        StringBuffer buffer = new StringBuffer();

        buffer.append("\nurl: ").append(doc.getUrl());
        buffer.append("\ncontentType: ").append(doc.getContentType());
        if (doc.getMetadata() != null && showMD) {
            buffer.append("\nmetadata: ");
            for (Entry<String, String> e : doc.getMetadata().entrySet()) {
                buffer.append("\n\t");
                buffer.append(e.getKey());
                buffer.append(": ");
                buffer.append(e.getValue());
            }
        }
        if (showContent && doc.getContent() != null) {
            buffer.append("\nContent:\n");
            int maxLengthText = Math.min(200, doc.getContent().remaining());
            byte[] b = new byte[maxLengthText];
            doc.getContent().get(b, 0, b.length);
            buffer.append(new String(b));
        }
        // try
        // default
        // encoding
        if (doc.getText() != null && showText) {
            buffer.append("\nText:\n");
            int maxLengthText = Math.min(200, doc.getText().length());
            buffer.append(doc.getText().substring(0, maxLengthText));
        }
        if (doc.getAnnotations() == null || !showAnnotations)
            return buffer.toString();
        buffer.append("\nAnnotations:\n");
        for (Annotation ann : doc.getAnnotations()) {
            buffer.append("\t").append(ann.toString()).append("\n");
        }

        return buffer.toString();
    }
}
