package com.digitalpebble.behemoth.io.nutch;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.behemoth.BehemothDocument;

public class NutchSegmentConverterMapper extends Mapper<Text, Content, Text, BehemothDocument> {
	
    public static final Logger LOG = LoggerFactory
    .getLogger(NutchSegmentConverterMapper.class);
	
    @Override
    public void map(Text key, Content content,
            Mapper<Text, Content, Text, BehemothDocument>.Context context)
            throws IOException, InterruptedException {

        BehemothDocument behemothDocument = new BehemothDocument();

        int status = Integer.parseInt(content.getMetadata().get(
                Nutch.FETCH_STATUS_KEY));
        if (status != CrawlDatum.STATUS_FETCH_SUCCESS) {
            // content not fetched successfully, skip document
            LOG.debug("Skipping " + key
                    + " as content is not fetched successfully");
            return;
        }

        // TODO store the fetch metadata in the Behemoth document
        // store the binary content and mimetype in the Behemoth document

        String contentType = content.getContentType();
        byte[] binarycontent = content.getContent();
        behemothDocument.setUrl(key.toString());
        behemothDocument.setContent(binarycontent);
        behemothDocument.setContentType(contentType);
        context.write(key, behemothDocument);
    }
}
