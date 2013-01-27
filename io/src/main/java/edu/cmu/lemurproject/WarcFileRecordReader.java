/**
 * A Hadoop record reader for reading Warc Records
 *
 * (C) 2009 - Carnegie Mellon University
 *
 * 1. Redistributions of this source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. The names "Lemur", "Indri", "University of Massachusetts",
 *    "Carnegie Mellon", and "lemurproject" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. To obtain permission, contact
 *    license@lemurproject.org.
 *
 * 4. Products derived from this software may not be called "Lemur" or "Indri"
 *    nor may "Lemur" or "Indri" appear in their names without prior written
 *    permission of The Lemur Project. To obtain permission,
 *    contact license@lemurproject.org.
 *
 * THIS SOFTWARE IS PROVIDED BY THE LEMUR PROJECT AS PART OF THE CLUEWEB09
 * PROJECT AND OTHER CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN
 * NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * @author mhoy@cs.cmu.edu (Mark J. Hoy)
 */

package edu.cmu.lemurproject;

import edu.cmu.lemurproject.WarcRecord;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

public class WarcFileRecordReader<K extends WritableComparable, V extends Writable>  implements RecordReader<LongWritable, WritableWarcRecord> {
  public static final Log LOG = LogFactory.getLog(WarcFileRecordReader.class);

  private long recordNumber=1;

  private Path[] filePathList=null;
  private int currentFilePath=-1;

  private FSDataInputStream currentFile=null;
  private CompressionCodec compressionCodec=null;
  private DataInputStream compressionInput=null;
  private FileSystem fs=null;
  private long totalFileSize=0;
  private long totalNumBytesRead=0;

  public WarcFileRecordReader(Configuration conf, InputSplit split) throws IOException {
    this.fs = FileSystem.get(conf);
    if (split instanceof FileSplit) {
      this.filePathList=new Path[1];
      this.filePathList[0]=((FileSplit)split).getPath();
    } else if (split instanceof MultiFileSplit) {
      this.filePathList=((MultiFileSplit)split).getPaths();
    } else {
      throw new IOException("InputSplit is not a file split or a multi-file split - aborting");
    }

    // get the total file sizes
    for (int i=0; i < filePathList.length; i++) {
      totalFileSize += fs.getFileStatus(filePathList[i]).getLen();
    }

    Class<? extends CompressionCodec> codecClass=null;

    try {
      codecClass=conf.getClassByName("org.apache.hadoop.io.compress.GzipCodec").asSubclass(CompressionCodec.class);
      compressionCodec=(CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
    } catch (ClassNotFoundException cnfEx) {
      compressionCodec=null;
      LOG.info("!!! ClassNotFoun Exception thrown setting Gzip codec");
    }

    openNextFile();
  }

  private boolean openNextFile() {
    try {
      if (compressionInput!=null) {
        compressionInput.close();
      } else if (currentFile!=null) {
        currentFile.close();
      }
      currentFile=null;
      compressionInput=null;

      currentFilePath++;
      if (currentFilePath >= filePathList.length) { return false; }

      currentFile=this.fs.open(filePathList[currentFilePath]);

      // is the file gzipped?
      if ((compressionCodec!=null) && (filePathList[currentFilePath].getName().endsWith("gz"))) {
        compressionInput=new DataInputStream(compressionCodec.createInputStream(currentFile));
        LOG.info("Compression enabled");
      }

    } catch (IOException ex) {
      LOG.info("IOError opening " + filePathList[currentFilePath].toString() + " - message: " + ex.getMessage());
      return false;
    }
    return true;
  }

  public boolean next(LongWritable key, WritableWarcRecord value) throws IOException {
    DataInputStream whichStream=null;
    if (compressionInput!=null) {
      whichStream=compressionInput;
    } else if (currentFile!=null) {
      whichStream=currentFile;
    }

    if (whichStream==null) { return false; }

    WarcRecord newRecord=WarcRecord.readNextWarcRecord(whichStream);
    if (newRecord==null) {
      // try advancing the file
      if (openNextFile()) {
        newRecord=WarcRecord.readNextWarcRecord(whichStream);
      }

      if (newRecord==null) { return false; }
    }

    totalNumBytesRead += (long)newRecord.getTotalRecordLength();
    newRecord.setWarcFilePath(filePathList[currentFilePath].toString());

    // now, set our output variables
    value.setRecord(newRecord);
    key.set(recordNumber);

    recordNumber++;
    return true;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public WritableWarcRecord createValue() {
    return new WritableWarcRecord();
  }

  public long getPos() throws IOException {
    return totalNumBytesRead;
  }

  public void close() throws IOException {
    totalNumBytesRead=totalFileSize;
    if (compressionInput!=null) {
      compressionInput.close();
    } else if (currentFile!=null) {
      currentFile.close();
    }
  }

  public float getProgress() throws IOException {
    if (compressionInput!=null) {
      if (filePathList.length==0) { return 1.0f; }
      // return which file - can't do extact byte matching
      return (float)currentFilePath / (float)(filePathList.length);
    }
    if (totalFileSize==0) { return 0.0f; }
    return (float)totalNumBytesRead/(float)totalFileSize;
  }

}
