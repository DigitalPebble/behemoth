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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implementation of an Annotation. Has a type , metadata and start and end
 * offsets referring to the position in the text of a @class BehemothDocument.
 **/
public class Annotation implements Comparable<Annotation> {

    private String type;

    private long start;

    private long end;

    private Map<String, String> features;

    public Annotation() {
        type = "";
        start = -1;
        end = -1;
    }

    public int getFeatureNum() {
        if (this.getFeatures() == null)
            return 0;
        return this.getFeatures().size();
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public Map<String, String> getFeatures() {
        if (features == null)
            features = new HashMap<String, String>();
        return features;
    }

    public void setFeatures(Map<String, String> features) {
        this.features = features;
    }

    // sort by start offset then type
    public int compareTo(Annotation target) {
        long diff = this.start - target.start;
        if (diff != 0)
            return (int) diff;
        diff = this.type.compareTo(target.type);
        if (diff != 0)
            return (int) diff;
        diff = this.end - target.end;
        if (diff != 0)
            return (int) diff;
        // eventually compare based on the features
        diff = this.getFeatureNum() - target.getFeatureNum();
        if (diff != 0)
            return (int) diff;
        // TODO compare the features one by one?
        return 0;
    }

    /** Returns a String representation of the Annotation **/
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.type).append("\t").append(start).append("\t")
                .append(end);
        if (features != null) {
            Iterator<String> keysiter = features.keySet().iterator();
            while (keysiter.hasNext()) {
                String key = keysiter.next();
                String value = features.get(key).toString();
                builder.append("\t").append(key).append("=").append(value);
            }
        }
        return builder.toString();
    }

}
