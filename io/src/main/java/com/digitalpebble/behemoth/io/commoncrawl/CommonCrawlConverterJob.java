package com.digitalpebble.behemoth.io.commoncrawl;


import com.digitalpebble.behemoth.BehemothDocument;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.protocol.shared.ArcFileItem;

import java.io.IOException;

/**
 *
 *
 **/
public class CommonCrawlConverterJob extends Configured implements Tool {

  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      String syntax = "hadoop jar job.jar " + CommonCrawlConverterJob.class.getName() + " input solrURL";
      System.err.println(syntax);
      return -1;
    }

    Path inputPath = new Path(args[0]);
    String solrURL = args[1];

    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName(getClass().getName());
    conf.setJarByClass(CommonCrawlConverterJob.class);

    // Input
    ARCInputFormat.setARCSourceClass(conf, ARCInputSource.class);
    FileInputFormat.setInputPaths(conf, inputPath);
    conf.setInputFormat(ARCInputFormat.class);
    //conf.setInputFormat(ArcInputFormat.class);

    // Output
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(BehemothDocument.class);

    // MapReduce
    conf.setMapperClass(ArcToBehemothTransformer.class);
    //conf.setMapperClass(NutchArcToBehemothTransformer.class);
    conf.setNumReduceTasks(0); // map-only

    // Solr
    conf.set("solr.server.url", solrURL);

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new CommonCrawlConverterJob(), args);
  }

}

class NutchArcToBehemothTransformer extends MapReduceBase implements Mapper<Text, BytesWritable, Text, BehemothDocument> {
  BehemothDocument doc = new BehemothDocument();


  public void map(Text key, BytesWritable value,
                  OutputCollector<Text, BehemothDocument> collector, Reporter reporter)
          throws IOException {
    String[] headers = key.toString().split("\\s+");
    String urlStr = headers[0];
    //String version = headers[2];
    String contentType = headers[3];
    doc.setContent(value.get());
    doc.setContentType(contentType);
    doc.setUrl(urlStr);
    collector.collect(key, doc);
  }
}

class ArcToBehemothTransformer extends MapReduceBase implements Mapper<Text, ArcFileItem, Text, BehemothDocument> {

  public void map(Text key, ArcFileItem doc,
                  OutputCollector<Text, BehemothDocument> collector, Reporter reported)
          throws IOException {
    BehemothDocument newDoc = new BehemothDocument();
    newDoc.setUrl(doc.getUri());
    newDoc.setContent(doc.getContent().getReadOnlyBytes());
    newDoc.setContentType(doc.getMimeType());
    collector.collect(key, newDoc);
  }

}


