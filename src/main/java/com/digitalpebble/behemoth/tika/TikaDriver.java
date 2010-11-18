package com.digitalpebble.behemoth.tika;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.uima.UIMAMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


/**
 *
 *
 **/
public class TikaDriver extends Configured implements Tool {
  public TikaDriver() {
    super(null);
  }

  public TikaDriver(Configuration conf) {
    super(conf);
  }


  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(BehemothConfiguration.create(),
            new TikaDriver(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    final FileSystem fs = FileSystem.get(getConf());

    if (args.length < 2) {
      String syntax = "com.digitalpebble.behemoth.tika.TikaDriver in out [FQN of TikaProcessor implementation]";
      System.err.println(syntax);
      return -1;
    }

    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    String handlerName = null;
    if (args.length >= 3){
      handlerName = args[3];
    }

    JobConf job = new JobConf(getConf());
    job.setJarByClass(this.getClass());
    if (handlerName != null && handlerName.equals("") == false) {
      job.set("tika.processor", handlerName);
    }
    job.setJobName("Processing with Tika");

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(BehemothDocument.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BehemothDocument.class);

    job.setMapperClass(TikaMapper.class);

    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    // push the UIMA pear onto the DistributedCache
    //DistributedCache.addCacheFile(new URI(pearPath), job);

    //job.set("uima.pear.path", pearPath);

    try {
      JobClient.runJob(job);
    } catch (Exception e) {
      e.printStackTrace();
      fs.delete(outputPath, true);
    } finally {
      //DistributedCache.purgeCache(job);
    }

    return 0;
  }

}
