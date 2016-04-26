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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.digitalpebble.behemoth.Annotation;

public class AnnotationsUtil {

    private AnnotationsUtil() {
    }

    /** Sort the annotations by startOffset **/
    public static void sort(List<Annotation> input) {
        Collections.sort(input, new AnnotationComparator());
    }

    /** Returns the annotations contained between start and end included **/
    public static List<Annotation> getContained(List<Annotation> input,
            long start, long end) {
        // BRUTAL
        List<Annotation> output = new ArrayList<Annotation>();
        Iterator<Annotation> iterator = input.iterator();
        while (iterator.hasNext()) {
            Annotation annot = iterator.next();
            if (annot.getStart() >= start && annot.getEnd() <= end)
                output.add(annot);
        }
        return output;
    }

    /**
     * Returns the annotations matching the type+feature?+value? e.g.
     * div.class=page The type, feature or value can be regular expressions.
     * feature or value can be null in which case we check only for the presence
     * of the type or feature.
     **/
    public static List<Annotation> filter(List<Annotation> input, String type,
            String feature, String value) {
        List<Annotation> output = new ArrayList<Annotation>();
        Iterator<Annotation> iterator = input.iterator();
        main: while (iterator.hasNext()) {
            Annotation annot = iterator.next();
            // TODO check that types are not null
            boolean hastypematch = annot.getType().matches(type);
            if (!hastypematch)
                continue;
            // check the features
            // TODO no feature? no worries
            if (feature == null) {
                output.add(annot);
                continue;
            }

            // find all the keys matching the regex
            Iterator<String> keyIter = annot.getFeatures().keySet().iterator();
            while (keyIter.hasNext()) {
                String key = keyIter.next();
                boolean keyMatch = key.matches(feature);
                if (!keyMatch)
                    continue;
                // has a value been specified?
                if (value == null | value.length() == 0) {
                    output.add(annot);
                    continue main;
                }
                // need to check whether the values match
                String val = annot.getFeatures().get(key);
                boolean valueMatch = val.matches(value);
                if (!valueMatch)
                    continue;
                {
                    output.add(annot);
                    continue main;
                }
            }
        }
        return output;
    }

}

class AnnotationComparator implements java.util.Comparator {

    public int compare(Object o1, Object o2) {
        if (!(o1 instanceof Annotation) || !(o2 instanceof Annotation))
            return 0;

        Annotation a1 = (Annotation) o1;
        Annotation a2 = (Annotation) o2;

        Long l1 = a1.getStart();
        Long l2 = a2.getStart();
        if (l1 != null)
            return l1.compareTo(l2);
        else
            return -1;
    }
}
