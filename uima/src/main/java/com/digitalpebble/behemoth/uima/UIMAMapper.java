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

package com.digitalpebble.behemoth.uima;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.pear.tools.PackageBrowser;
import org.apache.uima.pear.tools.PackageInstaller;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.FileUtils;
import org.apache.uima.util.XMLInputSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;

public class UIMAMapper extends UIMABase implements
        Mapper<Text, BehemothDocument, Text, BehemothDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(UIMAMapper.class);

    private List<Type> uimatypes = new ArrayList<Type>();

    private File installDir;

    public void map(Text id, BehemothDocument behemoth,
            OutputCollector<Text, BehemothDocument> output, Reporter reporter)
            throws IOException {

        reporter.setStatus("UIMA : " + id.toString());

        // generate a CAS from the input document
        cas.reset();

        try {
            doProcess(behemoth, reporter);
        } catch (Exception e) {
            reporter.incrCounter("UIMA", "Exception", 1);
            throw new IOException(e);
        }

        reporter.incrCounter("UIMA", "Document", 1);

        // dump the modified document
        output.collect(id, behemoth);
    }

    public void configure(JobConf conf) {

        this.config = conf;

        storeshortnames = config.getBoolean("uima.store.short.names", true);

        File pearpath = new File(conf.get("uima.pear.path"));
        String pearname = pearpath.getName();

        URL urlPEAR = null;

        try {
            Path[] localArchives = DistributedCache.getLocalCacheFiles(conf);
            // identify the right archive
            for (Path la : localArchives) {
                String localPath = la.toUri().toString();
                LOG.info("Inspecting local paths " + localPath);
                if (!localPath.endsWith(pearname))
                    continue;
                urlPEAR = new URL("file://" + localPath);
                break;
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Impossible to retrieve gate application from distributed cache",
                    e);
        }

        if (urlPEAR == null)
            throw new RuntimeException("UIMA pear " + pearpath
                    + " not available in distributed cache");

        File pearFile = new File(urlPEAR.getPath());

        // should check whether a different mapper has already unpacked it
        // but for now we just unpack in a different location for every mapper
        TaskAttemptID attempt = TaskAttemptID.forName(conf
                .get("mapred.task.id"));
        installDir = new File(pearFile.getParentFile(), attempt.toString());
        PackageBrowser instPear = PackageInstaller.installPackage(installDir,
                pearFile, true);

        // get the resources required for the AnalysisEngine
        org.apache.uima.resource.ResourceManager rsrcMgr = UIMAFramework
                .newDefaultResourceManager();

        newCAS(instPear, rsrcMgr);

        fillFeatFilts();

        String[] annotTypes = this.config.get("uima.annotations.filter", "")
                .split(",");
        uimatypes = new ArrayList<Type>(annotTypes.length);

        for (String type : annotTypes) {
            Type aType = cas.getTypeSystem().getType(type);
            uimatypes.add(aType);
        }

    }

    public void close() throws IOException {
        if (cas != null)
            cas.release();
        if (tae != null)
            tae.destroy();
        if (installDir != null) {
            FileUtils.deleteRecursive(installDir);
        }
    }

}
