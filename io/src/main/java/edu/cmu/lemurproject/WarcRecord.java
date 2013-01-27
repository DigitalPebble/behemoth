/*
Lemur License Agreement

  Copyright (c) 2000-2011 The Lemur Project.  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:

  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in
     the documentation and/or other materials provided with the
     distribution.

  3. The names "Lemur", "Indri", "University of Massachusetts" and
     "Carnegie Mellon" must not be used to endorse or promote products
     derived from this software without prior written permission. To
     obtain permission, contact license@lemurproject.org

  4. Products derived from this software may not be called "Lemur" or "Indri"
     nor may "Lemur" or "Indri" appear in their names without prior written
     permission of The Lemur Project. To obtain permission,
     contact license@lemurproject.org.

  THIS SOFTWARE IS PROVIDED BY THE LEMUR PROJECT AND OTHER
  CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
  BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
  FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
  COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
  INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
  BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
  OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
  TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
  USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
  DAMAGE.

 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.lemurproject;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;

/**
 *
 * @author mhoy
 */
public class WarcRecord {

  // public static final Log LOG = LogFactory.getLog(WarcRecord.class);
  
  public static String WARC_VERSION = "WARC/";
  public static String WARC_VERSION_LINE = "WARC/0.18\n";

  ////public static String WARC_VERSION = "WARC/1.0";
  //public static String WARC_VERSION = "WARC/0.18";
  ////public static String WARC_VERSION_LINE = "WARC/1.0\n";
  //public static String WARC_VERSION_LINE = "WARC/0.18\n";
  private static String NEWLINE="\n";
  private static String CR_NEWLINE="\r\n";
  
  private static byte MASK_THREE_BYTE_CHAR=(byte)(0xE0);
  private static byte MASK_TWO_BYTE_CHAR=(byte)(0xC0);
  private static byte MASK_TOPMOST_BIT=(byte)(0x80);
  private static byte MASK_BOTTOM_SIX_BITS=(byte)(0x1F);
  private static byte MASK_BOTTOM_FIVE_BITS=(byte)(0x3F);
  private static byte MASK_BOTTOM_FOUR_BITS=(byte)(0x0F);
  
  private static String LINE_ENDING="\n";
  
  private static String readLineFromInputStream(DataInputStream in) throws IOException {
    StringBuilder retString=new StringBuilder();
    boolean found_cr = false;
    boolean keepReading=true;
    try {
      do {
        char thisChar=0;
        byte readByte=in.readByte();
        // check to see if it's a multibyte character
        if ((readByte & MASK_THREE_BYTE_CHAR) == MASK_THREE_BYTE_CHAR) {
	    found_cr = false;
          // need to read the next 2 bytes
          if (in.available() < 2) {
            // treat these all as individual characters
            retString.append((char)readByte);
            int numAvailable=in.available();
            for (int i=0; i < numAvailable; i++) {
              retString.append((char)(in.readByte()));
            }
            continue;
          }
          byte secondByte=in.readByte();
          byte thirdByte=in.readByte();
          // ensure the topmost bit is set
          if (((secondByte & MASK_TOPMOST_BIT)!=MASK_TOPMOST_BIT) || ((thirdByte & MASK_TOPMOST_BIT)!=MASK_TOPMOST_BIT)) {
            //treat these as individual characters
            retString.append((char)readByte);
            retString.append((char)secondByte);
            retString.append((char)thirdByte);
            continue;
          }
          int finalVal=(thirdByte & MASK_BOTTOM_FIVE_BITS) + 64*(secondByte & MASK_BOTTOM_FIVE_BITS) + 4096*(readByte & MASK_BOTTOM_FOUR_BITS);
          thisChar=(char)finalVal;
        } else if ((readByte & MASK_TWO_BYTE_CHAR) == MASK_TWO_BYTE_CHAR) {
	    found_cr = false;

          // need to read next byte
          if (in.available() < 1) {
            // treat this as individual characters
            retString.append((char)readByte);
            continue;
          }
          byte secondByte=in.readByte();
          if ((secondByte & MASK_TOPMOST_BIT)!=MASK_TOPMOST_BIT) {
            retString.append((char)readByte);
            retString.append((char)secondByte);
            continue;
          }
          int finalVal=(secondByte & MASK_BOTTOM_FIVE_BITS) + 64*(readByte & MASK_BOTTOM_SIX_BITS);
          thisChar=(char)finalVal;
        } else {
          // interpret it as a single byte
          thisChar=(char)readByte;
        }
	// Look for carriage return; if found set a flag
	if (thisChar=='\r') {
	    found_cr = true;
	}
	if (thisChar=='\n') {
	    // if the linefeed is the next character after the carriage return
	    if (found_cr) {
		LINE_ENDING = CR_NEWLINE;
	    } else {
		LINE_ENDING = NEWLINE;
	    } 
	    keepReading=false;
        } else {
          retString.append(thisChar);
        }
      } while (keepReading);
    } catch (EOFException eofEx) {
      return null;
    }
    
    if (retString.length()==0) { 
      return "";
    }
    
    return retString.toString();
  }
  
  private static byte[] readNextRecord(DataInputStream in, StringBuffer headerBuffer) throws IOException {
    if (in==null) { return null; }
    if (headerBuffer==null) { return null; }

    String line=null;
    boolean foundMark=false;    
    byte[] retContent=null;

    // cannot be using a buffered reader here!!!!
    // just read the header
    // first - find our WARC header
    while ((!foundMark) && ((line=readLineFromInputStream(in))!=null)) {
      if (line.startsWith(WARC_VERSION)) {
        WARC_VERSION_LINE = line;
        foundMark=true;
      }
    }

    // no WARC mark?
    if (!foundMark) { return null; }
    
    // LOG.info("Found WARC_VERSION");

    int contentLength = -1;
    // read until we see contentLength then an empty line
    // (to handle malformed ClueWeb09 headers that have blank lines)
    // get the content length and set our retContent
    for (line = readLineFromInputStream(in).trim(); 
        line.length() > 0 || contentLength < 0; 
        line = readLineFromInputStream(in).trim()) {
      
      if (line.length() > 0 ) {
        headerBuffer.append(line);
        headerBuffer.append(LINE_ENDING);
        
        // find the content length designated by Content-Length: <length>
        String[] parts = line.split(":", 2);
        if (parts.length == 2 && parts[0].equals("Content-Length")) {
          try {
            contentLength=Integer.parseInt(parts[1].trim());
            // LOG.info("WARC record content length: " + contentLength);
          } catch (NumberFormatException nfEx) {
            contentLength=-1;
          }
        }
      }
    }
    
    // now read the bytes of the content
    retContent=new byte[contentLength];
    int totalWant=contentLength;
    int totalRead=0;
    //
    // LOOP TO REMOVE LEADING CR * LF 
    // To prevent last few characters from being cut off of the content
    // when reading
    //
    while ((totalRead == 0) && (totalRead < contentLength)) {
      byte CR = in.readByte();
      byte LF = in.readByte();
      if ((CR != 13) && (LF != 10)) {
        retContent[0] = CR;
        retContent[1] = LF;
        totalRead = 2;
        totalWant = contentLength - totalRead;
      }
    }
    //
    //
    //
    while (totalRead < contentLength) {
       try {
        int numRead=in.read(retContent, totalRead, totalWant);
        if (numRead < 0) {
          return null;
        } else {
          totalRead += numRead;
          totalWant = contentLength-totalRead;
        } // end if (numRead < 0) / else
      } catch (EOFException eofEx) {
        // resize to what we have
        if (totalRead > 0) {
          byte[] newReturn=new byte[totalRead];
          System.arraycopy(retContent, 0, newReturn, 0, totalRead);
          return newReturn;
        } else {
          return null;
        }
       } // end try/catch (EOFException)
    } // end while (totalRead < contentLength)

    return retContent;
  }
  
  public static WarcRecord readNextWarcRecord(DataInputStream in) throws IOException {
    // LOG.info("Starting read of WARC record");
    StringBuffer recordHeader=new StringBuffer();
    byte[] recordContent=readNextRecord(in, recordHeader);
    if (recordContent==null) { 
      // LOG.info("WARC content is null - file is complete");
      return null; 
    }
    
    // extract out our header information
    String thisHeaderString=recordHeader.toString();


    String[] headerLines=thisHeaderString.split(LINE_ENDING);

    WarcRecord retRecord=new WarcRecord();
    for (int i=0; i < headerLines.length; i++) {
      String[] pieces=headerLines[i].split(":", 2);
      if (pieces.length!=2) { 
        retRecord.addHeaderMetadata(pieces[0], "");
        continue; 
      }
      String thisKey=pieces[0].trim();
      String thisValue=pieces[1].trim();

      // check for known keys
      if (thisKey.equals("WARC-Type")) { 
        // LOG.info("Setting WARC record type: " + thisValue);
        retRecord.setWarcRecordType(thisValue);
      } else if (thisKey.equals("WARC-Date")) {
        retRecord.setWarcDate(thisValue);
      } else if (thisKey.equals("WARC-Record-ID")) {
        // LOG.info("Setting WARC record ID: " + thisValue);
        retRecord.setWarcUUID(thisValue);
      } else if (thisKey.equals("Content-Type")) {
        retRecord.setWarcContentType(thisValue);
      } else {
        retRecord.addHeaderMetadata(thisKey, thisValue);
      }
    }

    // set the content
    retRecord.setContent(recordContent);
    
    return retRecord;
  }
  
  public class WarcHeader {
    public String contentType="";
    public String UUID="";
    public String dateString="";
    public String recordType="";
    public HashMap<String, String> metadata=new HashMap<String, String>();
    public int contentLength=0;
    
    public WarcHeader() {
    }
    
    public WarcHeader(WarcHeader o) {
      this.contentType=o.contentType;
      this.UUID=o.UUID;
      this.dateString=o.dateString;
      this.recordType=o.recordType;
      this.metadata.putAll(o.metadata);
      this.contentLength=o.contentLength;
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeUTF(contentType);
      out.writeUTF(UUID);
      out.writeUTF(dateString);
      out.writeUTF(recordType);
      out.writeInt(metadata.size());
      Iterator<Entry<String,String>> metadataIterator=metadata.entrySet().iterator();
      while (metadataIterator.hasNext()) {
        Entry<String,String> thisEntry=metadataIterator.next();
        out.writeUTF(thisEntry.getKey());
        out.writeUTF(thisEntry.getValue());
      }
      out.writeInt(contentLength);
    }
    
    public void readFields(DataInput in) throws IOException {
      contentType=in.readUTF();
      UUID=in.readUTF();
      dateString=in.readUTF();
      recordType=in.readUTF();
      metadata.clear();
      int numMetaItems=in.readInt();
      for (int i=0; i < numMetaItems; i++) {
        String thisKey=in.readUTF();
        String thisValue=in.readUTF();
        metadata.put(thisKey, thisValue);
      }
      contentLength=in.readInt();
    }
    
    @Override
    public String toString() {
      StringBuffer retBuffer=new StringBuffer();
      
      retBuffer.append(WARC_VERSION_LINE);
      retBuffer.append(LINE_ENDING);
      
      retBuffer.append("WARC-Type: " + recordType + LINE_ENDING);
      retBuffer.append("WARC-Date: " + dateString + LINE_ENDING);
      
      Iterator<Entry<String,String>> metadataIterator=metadata.entrySet().iterator();
      while (metadataIterator.hasNext()) {
        Entry<String,String> thisEntry=metadataIterator.next();
        retBuffer.append(thisEntry.getKey());
        retBuffer.append(": ");
        retBuffer.append(thisEntry.getValue());
        retBuffer.append(LINE_ENDING);
      }
      // Keep this as the last WARC-...
      retBuffer.append("WARC-Record-ID: " + UUID + LINE_ENDING);
      
      retBuffer.append("Content-Type: " + contentType + LINE_ENDING);
      retBuffer.append("Content-Length: " + contentLength + LINE_ENDING);
      
      return retBuffer.toString();
    }
  }

  private WarcHeader warcHeader=new WarcHeader();
  private byte[] warcContent=null;
  private String warcFilePath="";
  
  public WarcRecord() {
    
  }
  
  public WarcRecord(WarcRecord o) {
    this.warcHeader=new WarcHeader(o.warcHeader);
    this.warcContent=o.warcContent;
  }
  
  public int getTotalRecordLength() {
    int headerLength=warcHeader.toString().length();
    return (headerLength + warcContent.length);
  }
  
  public void set(WarcRecord o) {
    this.warcHeader=new WarcHeader(o.warcHeader);
    this.warcContent=o.warcContent;
  }
  
  public String getWarcFilePath() {
    return warcFilePath;
  }
  
  public void setWarcFilePath(String path) {
    warcFilePath=path;
  }
  
  public void setWarcRecordType(String recordType) {
    warcHeader.recordType=recordType;
  }
  
  public void setWarcContentType(String contentType) {
    warcHeader.contentType=contentType;
  }
  
  public void setWarcDate(String dateString) {
    warcHeader.dateString=dateString;
  }
  
  public void setWarcUUID(String UUID) {
    warcHeader.UUID=UUID;
  }
  
  public void addHeaderMetadata(String key, String value) {
    // don't allow addition of known keys
    if (key.equals("WARC-Type")) { return; }
    if (key.equals("WARC-Date")) { return; }
    if (key.equals("WARC-Record-ID")) { return; }
    if (key.equals("Content-Type")) { return; }
    if (key.equals("Content-Length")) { return; }
    
    warcHeader.metadata.put(key, value);
  }

  
  public void clearHeaderMetadata() {
    warcHeader.metadata.clear();
  }
  
  public Set<Entry<String,String>> getHeaderMetadata() {
    return warcHeader.metadata.entrySet();
  }
  
  public String getHeaderMetadataItem(String key) {
    
    if (key.equals("WARC-Type")) { return warcHeader.recordType; }
    if (key.equals("WARC-Date")) { return warcHeader.dateString; }
    if (key.equals("WARC-Record-ID")) { return warcHeader.UUID; }
    if (key.equals("Content-Type")) { return warcHeader.contentType; }
    if (key.equals("Content-Length")) { return Integer.toString(warcHeader.contentLength); }

    return warcHeader.metadata.get(key);
  }
  
  public void setContent(byte[] content) {
    warcContent=content;
    warcHeader.contentLength=content.length;
  }
  
  public void setContent(String content) {
    setContent(content.getBytes());
  }
    public void setContentLength(int len) {
        warcHeader.contentLength=len;
  }
  
  public byte[] getContent() {
    return warcContent;
  }
  public byte[] getByteContent() {
    return warcContent;
  }
 
  public String getContentUTF8() {
    String retString=null;
    try {
      retString = new String(warcContent, "UTF-8");
    } catch (UnsupportedEncodingException ex) {
      retString=new String(warcContent);
    }
    return retString;
  }
  
  public String getHeaderRecordType() {
    return warcHeader.recordType;
  }
  
  @Override
  public String toString() {
    StringBuffer retBuffer=new StringBuffer();
    retBuffer.append(warcHeader.toString());
    retBuffer.append(LINE_ENDING);
    retBuffer.append(new String(warcContent));
    return retBuffer.toString();
  }

  public String getHeaderString() {
    return warcHeader.toString();
  }

  public void write(DataOutput out) throws IOException {
    warcHeader.write(out);
    out.write(warcContent);
  }
  
  public void readFields(DataInput in) throws IOException {
    warcHeader.readFields(in);
    int contentLengthBytes=warcHeader.contentLength;
    warcContent=new byte[contentLengthBytes];
    in.readFully(warcContent);
  }
  
}

