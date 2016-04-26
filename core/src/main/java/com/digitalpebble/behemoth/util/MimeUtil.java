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

package com.digitalpebble.behemoth.util;

public final class MimeUtil {

    private MimeUtil() {
    }

    private static final String SEPARATOR = ";";

    /**
     * Cleans a MimeType name by removing out the actual MimeType, from a string
     * of the form:
     * 
     * <pre>
     *      &lt;primary type&gt;/&lt;sub type&gt; ; &lt; optional params
     * </pre>
     * 
     * @param origType
     *            The original mime type string to be cleaned.
     * @return The primary type, and subtype, concatenated, e.g., the actual
     *         mime type.
     */
    public static String cleanMimeType(String origType) {
        if (origType == null)
            return null;

        // take the origType and split it on ';'
        String[] tokenizedMimeType = origType.split(SEPARATOR);
        if (tokenizedMimeType.length > 1) {
            // there was a ';' in there, take the first value
            return tokenizedMimeType[0];
        } else {
            // there wasn't a ';', so just return the orig type
            return origType;
        }
    }

}
