package com.digitalpebble.behemoth.io.sequencefile;

import com.digitalpebble.behemoth.BehemothDocument;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;

/**
 *
 *
 **/
public class SequenceFileConverterMapperTest {

    @Test
    public void test() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File tmpLoc = new File(tmpDir, "sfcmt");
        tmpLoc.mkdirs();

        Path in = new Path(tmpLoc.getAbsolutePath(), "foo");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, in,
                Text.class, Text.class);
        for (int i = 0; i < 100; i++) {
            writer.append(new Text(String.valueOf(i)), new Text(
                    "This is document " + i));
        }
        writer.close();
        SequenceFileConverterMapper mapper = new SequenceFileConverterMapper();
        SequenceFileIterator<Text, Text> iter = new SequenceFileIterator<Text, Text>(
                in, true, conf);
        DummyRecordWriter out = new DummyRecordWriter();
        Mapper.Context context = mapper.new Context(conf, new TaskAttemptID(),
                null, out, null, new DummyStatusReporter(), null);
        while (iter.hasNext()) {
            Pair<Text, Text> next = iter.next();
            mapper.map(next.getFirst(), next.getSecond(), context);
        }
        assertEquals(100, out.keys.size());
    }

    @Test
    public void testWritable() throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        File tmpDir = new File(System.getProperty("java.io.tmpdir"));
        File tmpLoc = new File(tmpDir, "sfcmt");
        tmpLoc.mkdirs();

        Path in = new Path(tmpLoc.getAbsolutePath(), "foo");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, in,
                Text.class, MyWritable.class);
        for (int i = 0; i < 100; i++) {
            writer.append(new Text(String.valueOf(i)), new MyWritable(i,
                    "document " + i));
        }
        writer.close();
        SequenceFileConverterMapper mapper = new SequenceFileConverterMapper();
        SequenceFileIterator<Text, Text> iter = new SequenceFileIterator<Text, Text>(
                in, true, conf);
        DummyRecordWriter out = new DummyRecordWriter();
        Mapper.Context context = mapper.new Context(conf, new TaskAttemptID(),
                null, out, null, new DummyStatusReporter(), null);
        while (iter.hasNext()) {
            Pair<Text, Text> next = iter.next();
            mapper.map(next.getFirst(), next.getSecond(), context);
        }
        assertEquals(100, out.keys.size());
        int i = 0;
        for (BehemothDocument doc : out.vals) {
            byte[] bytes = doc.getContent();
            DataInput input = new DataInputStream(new ByteArrayInputStream(
                    bytes));
            MyWritable writable = new MyWritable();
            writable.readFields(input);
            assertEquals(writable.i, i);
            assertEquals(writable.in, "document " + i);
            i++;
        }
    }

    private static class DummyRecordWriter extends
            RecordWriter<Text, BehemothDocument> {
        List<Text> keys = new ArrayList<Text>();
        List<BehemothDocument> vals = new ArrayList<BehemothDocument>();

        @Override
        public void write(Text text, BehemothDocument behemothDocument)
                throws IOException, InterruptedException {
            keys.add(text);
            vals.add(behemothDocument);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {

        }
    }

    // From Mahout
    private class DummyStatusReporter extends StatusReporter {
        private final Map<Enum<?>, Counter> counters = Maps.newHashMap();
        private final Map<String, Counter> counterGroups = Maps.newHashMap();

        @Override
        public Counter getCounter(Enum<?> name) {
            if (!counters.containsKey(name)) {
                counters.put(name, new DummyCounter());
            }
            return counters.get(name);
        }

        @Override
        public Counter getCounter(String group, String name) {
            if (!counterGroups.containsKey(group + name)) {
                counterGroups.put(group + name, new DummyCounter());
            }
            return counterGroups.get(group + name);
        }

        @Override
        public void progress() {
        }

        @Override
        public void setStatus(String status) {
        }

        @Override
        public float getProgress() {
          return 0;
        }

    }

}

final class DummyCounter extends Counter {

}
