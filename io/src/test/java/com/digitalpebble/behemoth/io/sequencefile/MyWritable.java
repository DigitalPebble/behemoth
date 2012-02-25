package com.digitalpebble.behemoth.io.sequencefile;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
*
*
**/
public class MyWritable implements Writable {
  int i;
  String in;

  public MyWritable() {
  }

  public MyWritable(int i, String in) {
    this.i = i;
    this.in = in;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(i);
    out.writeUTF(in);

  }

  public void readFields(DataInput in) throws IOException {
    i = in.readInt();
    this.in = in.readUTF();
  }
}
