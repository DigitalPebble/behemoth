package com.digitalpebble.behemoth.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.digitalpebble.behemoth.BehemothConfiguration;
import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Utility class used to read the content of a Behemoth SequenceFile.
 **/
public class CorpusReader extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(BehemothConfiguration.create(), new CorpusReader(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    Path input = new Path(args[0]);

    Reader[] cacheReaders = SequenceFileOutputFormat.getReaders(getConf(), input);
    for (Reader current : cacheReaders) {
      // read the key + values in that file
      Text key = new Text();
      BehemothDocument value = new BehemothDocument();
      while (current.next(key, value)) {
        System.out.println(value.toString());
      }
      current.close();
    }

    return 0;
  }

}
