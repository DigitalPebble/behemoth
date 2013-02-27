package com.digitalpebble.behemoth.tika;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class LucidWriteOutContentHandler extends DefaultHandler {

  /**
   * The character stream.
   */
  private final Writer writer;

  /**
   * The maximum number of characters to write to the character stream.
   * Set to -1 for no limit.
   */
  private final int writeLimit;

  /**
   * Number of characters written so far.
   */
  private int writeCount = 0;

  private LucidWriteOutContentHandler(Writer writer, int writeLimit) {
      this.writer = writer;
      this.writeLimit = writeLimit;
  }

  /**
   * Creates a content handler that writes character events to
   * the given writer.
   *
   * @param writer writer
   */
  public LucidWriteOutContentHandler(Writer writer) {
      this(writer, -1);
  }

  /**
   * Creates a content handler that writes character events to
   * the given output stream using the default encoding.
   *
   * @param stream output stream
   */
  public LucidWriteOutContentHandler(OutputStream stream, Charset charset) {
      this(new OutputStreamWriter(stream, charset));
  }

  /**
   * Creates a content handler that writes character events
   * to an internal string buffer. Use the {@link #toString()}
   * method to access the collected character content.
   * <p>
   * The internal string buffer is bounded at the given number of characters.
   * If this write limit is reached, then a {@link SAXException} is thrown.
   * The {@link #isWriteLimitReached(Throwable)} method can be used to
   * detect this case.
   *
   * @since Apache Tika 0.7
   * @param writeLimit maximum number of characters to include in the string,
   *                   or -1 to disable the write limit
   */
  public LucidWriteOutContentHandler(int writeLimit) {
      this(new StringWriter(), writeLimit);
  }

  /**
   * Creates a content handler that writes character events
   * to an internal string buffer. Use the {@link #toString()}
   * method to access the collected character content.
   * <p>
   * The internal string buffer is bounded at 100k characters. If this
   * write limit is reached, then a {@link SAXException} is thrown. The
   * {@link #isWriteLimitReached(Throwable)} method can be used to detect
   * this case.
   */
  public LucidWriteOutContentHandler() {
      this(100 * 1000);
  }

  /**
   * Writes the given characters to the given character stream.
   */
  @Override
  public void characters(char[] ch, int start, int length)
          throws SAXException {
      try {
          if (writeLimit == -1 || writeCount + length <= writeLimit) {
              writer.write(ch, start, length);
              writeCount += length;
          } else {
              writer.write(ch, start, writeLimit - writeCount);
              writeCount = writeLimit;
              throw new WriteLimitReachedException();
          }
      } catch (IOException e) {
          throw new SAXException("Error writing out character content", e);
      }
  }


  /**
   * Writes the given ignorable characters to the given character stream.
   */
  @Override
  public void ignorableWhitespace(char[] ch, int start, int length)
          throws SAXException {
      characters(ch, start, length);
  }

  /**
   * Flushes the character stream so that no characters are forgotten
   * in internal buffers.
   *
   * @see <a href="https://issues.apache.org/jira/browse/TIKA-179">TIKA-179</a>
   * @throws SAXException if the stream can not be flushed
   */
  @Override
  public void endDocument() throws SAXException {
      try {
          writer.flush();
      } catch (IOException e) {
          throw new SAXException("Error flushing character output", e);
      }
  }

  /**
   * Returns the contents of the internal string buffer where
   * all the received characters have been collected. Only works
   * when this object was constructed using the empty default
   * constructor or by passing a {@link StringWriter} to the
   * other constructor.
   */
  @Override
  public String toString() {
      return writer.toString();
  }

  /**
   * Checks whether the given exception (or any of it's root causes) was
   * thrown by this handler as a signal of reaching the write limit.
   *
   * @since Apache Tika 0.7
   * @param t throwable
   * @return <code>true</code> if the write limit was reached,
   *         <code>false</code> otherwise
   */
  public boolean isWriteLimitReached(Throwable t) {
      if (t instanceof WriteLimitReachedException) {
          return this == ((WriteLimitReachedException) t).getSource();
      } else {
          return t.getCause() != null && isWriteLimitReached(t.getCause());
      }
  }

  /**
   * The exception used as a signal when the write limit has been reached.
   */
  private class WriteLimitReachedException extends SAXException {

      public LucidWriteOutContentHandler getSource() {
          return LucidWriteOutContentHandler.this;
      }

  }

}
