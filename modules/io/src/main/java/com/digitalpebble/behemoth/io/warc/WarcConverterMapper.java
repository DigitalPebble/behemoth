package com.digitalpebble.behemoth.io.warc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.protocol.ProtocolException;

import com.digitalpebble.behemoth.BehemothDocument;

public class WarcConverterMapper extends MapReduceBase implements
        Mapper<LongWritable, WritableWarcRecord, Text, BehemothDocument> {
  
    private Text newKey = new Text();

    public void map(LongWritable key, WritableWarcRecord record,
            OutputCollector<Text, BehemothDocument> output, Reporter reporter)
            throws IOException {

        BehemothDocument behemothDocument = new BehemothDocument();

        if (record.getRecord().getHeaderRecordType().equals("response") == false)
            return;

        byte[] binarycontent = record.getRecord().getContent();

        String uri = record.getRecord()
                .getHeaderMetadataItem("WARC-Target-URI");
        // application/http;msgtype=response
        // but always null?
        // String WARCContentType =
        // record.getRecord().getHeaderMetadataItem("Content-Type");

        HttpResponse response;
        try {
            response = new HttpResponse(binarycontent);
        } catch (ProtocolException e) {
            return;
        }

        behemothDocument.setUrl(uri);
        newKey.set(uri);

        String contentType = response.getHeader(HttpHeaders.CONTENT_TYPE);
        behemothDocument.setContentType(contentType);
        behemothDocument.setContent(response.getContent());

        output.collect(newKey, behemothDocument);
    }
    
}
