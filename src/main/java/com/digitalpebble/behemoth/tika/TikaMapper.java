package com.digitalpebble.behemoth.tika;

import com.digitalpebble.behemoth.BehemothDocument;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * Uses a {@link com.digitalpebble.behemoth.tika.TikaProcessor} to extract text using Tika.  Users wanting to
 * override the default work of the TikaProcessor can set the "tika.processor" value in the JobConf and give it a
 * fully qualified class name.  The implementation must extend TikaProcessor and it must have a zero arg. constructor.
 */
public class TikaMapper extends MapReduceBase implements
        Mapper<Text, BehemothDocument, Text, BehemothDocument> {
  private static final Logger LOG = LoggerFactory.getLogger(TikaMapper.class);

  protected TikaProcessor processor;

  @Override
  public void map(Text text, BehemothDocument inputDoc,
                  OutputCollector<Text, BehemothDocument> outputCollector, Reporter reporter) throws IOException {

    BehemothDocument[] documents = processor.process(inputDoc, reporter);
    if (documents != null) {
      for (int i = 0; i < documents.length; i++) {
        outputCollector.collect(text, documents[i]);
      }
    }
  }

  @Override
  public void configure(JobConf job) {

    String handlerName = job.get("tika.processor");
    if (handlerName != null) {
      Class handlerClass = job.getClass(handlerName, TikaProcessor.class);
      try {
        processor = (TikaProcessor) handlerClass.newInstance();

      } catch (InstantiationException e) {
        LOG.error("Exception", e);
        //TODO: what's the best way to do this?
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        LOG.error("Exception", e);
        throw new RuntimeException(e);
      }
    } else {
      processor = new TikaProcessor();
    }
    processor.setConf(job);
  }
}
