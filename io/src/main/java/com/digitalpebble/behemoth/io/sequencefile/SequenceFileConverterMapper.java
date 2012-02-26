package com.digitalpebble.behemoth.io.sequencefile;

import com.digitalpebble.behemoth.BehemothDocument;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 *
 **/
public class SequenceFileConverterMapper extends
        Mapper<Writable, Writable, Text, BehemothDocument> {
    @Override
    protected void map(Writable key, Writable value, Context context)
            throws IOException, InterruptedException {
        BehemothDocument doc = new BehemothDocument();
        doc.setUrl(key.toString());
        // TODO: Is this the right way to do this? We need the bytes.
        DataOutputBuffer out = new DataOutputBuffer();
        value.write(out);
        doc.setContent(out.getData());
        // doc.setContent(value.toString().getBytes(Charset.forName("UTF-8")));
        context.write(new Text(key.toString()), doc);
    }
}
