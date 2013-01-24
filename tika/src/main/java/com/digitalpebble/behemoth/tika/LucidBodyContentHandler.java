package com.digitalpebble.behemoth.tika;

import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.Charset;

import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.apache.tika.sax.XHTMLContentHandler;

public class LucidBodyContentHandler extends ContentHandlerDecorator {

  /**
   * XHTML XPath parser.
   */
  private static final XPathParser PARSER =
    new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  /**
   * The XPath matcher used to select the XHTML body contents.
   */
  private static final Matcher MATCHER =
    PARSER.parse("/xhtml:html/xhtml:body/descendant:node()");

  /**
   * Creates a content handler that passes all XHTML body events to the
   * given underlying content handler.
   *
   * @param handler content handler
   */
  public LucidBodyContentHandler(ContentHandler handler) {
    super(new MatchingContentHandler(handler, MATCHER));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * the given writer.
   *
   * @param writer writer
   */
  public LucidBodyContentHandler(Writer writer) {
    this(new LucidWriteOutContentHandler(writer));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * the given output stream using the specified encoding.
   *
   * @param stream output stream
   */
  public LucidBodyContentHandler(OutputStream stream, String encoding) {
    this(new LucidWriteOutContentHandler(stream, Charset.forName(encoding)));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * the given output stream using the specified Charset.
   *
   * @param stream output stream
   */
  public LucidBodyContentHandler(OutputStream stream, Charset charset) {
    this(new LucidWriteOutContentHandler(stream, charset));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * an internal string buffer. The contents of the buffer can be retrieved
   * using the {@link #toString()} method.
   * <p>
   * The internal string buffer is bounded at the given number of characters.
   * If this write limit is reached, then a {@link SAXException} is thrown.
   *
   * @since Apache Tika 0.7
   * @param writeLimit maximum number of characters to include in the string,
   *                   or -1 to disable the write limit
   */
  public LucidBodyContentHandler(int writeLimit) {
    this(new LucidWriteOutContentHandler(writeLimit));
  }

  /**
   * Creates a content handler that writes XHTML body character events to
   * an internal string buffer. The contents of the buffer can be retrieved
   * using the {@link #toString()} method.
   * <p>
   * The internal string buffer is bounded at 100k characters. If this write
   * limit is reached, then a {@link SAXException} is thrown.
   */
  public LucidBodyContentHandler() {
    this(new LucidWriteOutContentHandler());
  }

}
