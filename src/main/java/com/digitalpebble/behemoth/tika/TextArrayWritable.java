package com.digitalpebble.behemoth.tika;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 *
 *
 **/
public class TextArrayWritable implements Writable {
  //Hmm, is this the best way to do this?
  private String[] array;

  public TextArrayWritable(String [] array){
    this.array = array;
  }

  public String[] getArray() {
    return array;
  }

  public void setArray(String[] array) {
    this.array = array;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    if (array != null) {
      dataOutput.writeInt(array.length);
      for (int i = 0; i < array.length; i++) {
        dataOutput.writeUTF(array[i]);
      }
    } else {
      dataOutput.writeInt(0);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    int len = dataInput.readInt();
    if (len > 0){
      array = new String[len];
      for (int i = 0; i < len; i++){
        array[i] = dataInput.readUTF();
      }
    }
  }
}
