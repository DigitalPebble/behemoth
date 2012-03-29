package com.digitalpebble.behemoth.io.commoncrawl;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.commoncrawl.hadoop.io.ARCResource;
import org.commoncrawl.hadoop.io.ARCSource;
import org.commoncrawl.hadoop.io.ARCSplitCalculator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;

/**
 *
 *
 **/
public class ARCInputSource extends ARCSplitCalculator implements ARCSource, JobConfigurable {
    private JobConf jc;

  @Override
  protected void configureImpl(JobConf job) {
    jc = job;
  }


  public InputStream getStream(String resource, long streamPosition,
                               Throwable lastError, int previousFailures) throws Throwable {

    if (lastError != null || previousFailures > 0) {
      // Don't retry...local IO failures are not expected
      return null;
    }

    if (streamPosition != 0) {
      // This shouldn't happen, but we'll check just in case
      throw new RuntimeException("Non-zero position requested");
    }

    if (jc == null) {
      throw new NullPointerException("Jc is null");
    }

    FileSystem fs = FileSystem.get(jc);
    System.err.println("getStream:: Opening: " + resource);
    FSDataInputStream is = fs.open(new Path(resource));
    is.seek(streamPosition);
    return is;
  }

  @Override
  protected Collection<ARCResource> getARCResources(JobConf job)
          throws IOException {
    Path[] inputPaths = FileInputFormat.getInputPaths(job);
    LinkedList<ARCResource> arc_resources = new LinkedList<ARCResource>();
    LinkedList<FileStatus> directories = new LinkedList<FileStatus>();

    FileSystem fs = FileSystem.get(job);
    for (Path inputPath : inputPaths) {
      FileStatus fstat = fs.getFileStatus(inputPath);
      if (fstat.isDir()) {
        for (FileStatus children : fs.listStatus(inputPath)) {
          directories.add(children);
        }
      } else {
        arc_resources.add(new ARCResource(inputPath.toUri().toASCIIString(), fstat.getLen()));
      }
    }

    while (!directories.isEmpty()) {
      FileStatus node = directories.pop();
      if (node.isDir()) {
        for (FileStatus children : fs.listStatus(node.getPath())) {
          directories.add(children);
        }
      } else {
        arc_resources.add(new ARCResource(node.getPath().toUri().toASCIIString(), node.getLen()));
      }
    }

    return arc_resources;
  }

}
