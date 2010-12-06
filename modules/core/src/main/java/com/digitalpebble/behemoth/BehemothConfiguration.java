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

public class BehemothConfiguration extends org.apache.hadoop.conf.Configuration {

    private BehemothConfiguration() {
    }

    /** Create a {@link Configuration} for Behemoth. */
    public static Configuration create() {
        Configuration conf = new Configuration();
        addBehemothResources(conf);
        return conf;
    }

    /** Add the standard Behemoth resources to {@link Configuration}. */
    private static Configuration addBehemothResources(Configuration conf) {
        conf.addResource("behemoth-default.xml");
        conf.addResource("behemoth-site.xml");
        return conf;
    }

}
