/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.behemoth.tika;

import com.digitalpebble.behemoth.BehemothDocument;
import com.digitalpebble.behemoth.DocumentProcessor;

import org.apache.commons.io.input.TeeInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.parser.html.HtmlMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

/**
 * Tika as a document processor. This implementation follows
 * the behavior of TikaParserController in LucidWorks (Shared).
 */

public class LucidWorksProcessor implements DocumentProcessor, TikaConstants {

  private static final Logger LOG = LoggerFactory
          .getLogger(LucidWorksProcessor.class);

  private Configuration config;
  private boolean includeMetadata = false;
  private String mimeType = "text/plain";
  public static final String defaultInputEncoding = "UTF-8";
  public static final String defaultOutputEncoding = "UTF-8";
  
  public static final String BODY_FIELD = "body";
  public static final String CONTAINER_FIELD = "belongsToContainer"; // Aperture-compatible name
  public static final String RESOURCE_FIELD = "resource_name";
  public static final String CRAWL_URI_FIELD = "crawl_uri";
  public static final String BATCH_ID_FIELD = "batch_id";

  protected String defaultFieldName = null;
  protected String batchId = null;
  protected ContentHandler dummy = new DefaultHandler();
  protected boolean includeImages = true;
  protected boolean flattenCompound = false;
  protected boolean addFailedDocs = true;
  protected boolean addOriginalContent = false;
  protected AutoDetectParser autoDetectParser = new AutoDetectParser();

  public Configuration getConf() {
    return config;
  }

  public void setConf(Configuration conf) {
    config = conf;
    mimeType = config.get(TIKA_MIME_TYPE_KEY);
    batchId = conf.get("lw.batch.id");
    includeMetadata = conf.getBoolean("tika.metadata", true);
    flattenCompound = conf.getBoolean("tika.flatten", false);
    addFailedDocs = conf.getBoolean("tika.add.failed", true);
    includeImages = conf.getBoolean("tika.images", true);
    addOriginalContent = conf.getBoolean("tika.add.original", false);
    autoDetectParser.setFallback(ErrorParser.INSTANCE);
  }

  public void close() {
  }

  /**
   * Process a BehemothDocument with Tika
   *
   * @return an array of documents or null if an exception is encountered
   */
  public BehemothDocument[] process(BehemothDocument inputDoc,
                                    Reporter reporter) {
    // check that it has some text or content
    if (inputDoc.getContent() == null && inputDoc.getText() == null) {
      LOG.info("No content or text for " + inputDoc.getUrl()
              + " skipping");
      setMetadata(inputDoc, "parsing", "skipped, no content");
      if (reporter != null)
        reporter.getCounter("TIKA", "DOC-NO_DATA").increment(1);
      return new BehemothDocument[]{inputDoc};
    }

    Metadata metadata = new Metadata();

    long len = inputDoc.getContent() != null ? inputDoc.getContent().length : 0;
    metadata.set(Metadata.CONTENT_LENGTH, Long.toString(len));

    // Seed parser's metadata with metadata from content
    for (Entry<Writable,Writable> e : inputDoc.getMetadata().entrySet()) {
      metadata.add(e.getKey().toString(), e.getValue().toString());
    }
    ParseContext context = new ParseContext();
    // put the mimetype in the metadata so that Tika can
    // decide which parser to use
    metadata.set(Metadata.CONTENT_TYPE, inputDoc.getContentType());

    if (reporter != null)
      reporter.getCounter("MIME-TYPE", inputDoc.getContentType())
              .increment(1);
    String docUrl = inputDoc.getUrl();
    String uniqueKey = "id"; // XXX
    List<BehemothDocument> docs = new LinkedList<BehemothDocument>();
    CollectingParser parser = new CollectingParser(autoDetectParser, docs,
            metadata, batchId, uniqueKey, docUrl, includeImages,
            flattenCompound, addOriginalContent, addFailedDocs);
    context.set(Parser.class, parser);
    context.set(AutoDetectParser.class, autoDetectParser);

    // TODO: How to get any parsing exceptions reported in an admin/user-friendly manner
    if (inputDoc.getContent() != null) {
      InputStream input = new ByteArrayInputStream(inputDoc.getContent());
      try {
        parser.parse(input, dummy, metadata, context);
        for (BehemothDocument d : docs) {
          setMetadata(d, "parsing", "ok");
          if (reporter != null)
            reporter.getCounter("TIKA", "DOC-PARSED").increment(1);
        }
      } catch (Throwable t) {
        LOG.info("Parsing failed for " + inputDoc.getUrl() + ", skipping: " + t.toString());
        if (addFailedDocs) {
          // add an empty doc with metadata only
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          t.printStackTrace(pw);
          pw.flush();
          setMetadata(inputDoc, "parsing", "failed: " + sw.toString());
          docs.add(inputDoc);
          if (reporter != null)
            reporter.getCounter("TIKA", "DOC-FAILED").increment(1);
        }
      } finally {
        try {
          if (input != null) {
            input.close();
          }
        } catch (IOException ioe) {
          //
        }
      }
    } else if (addFailedDocs) {
      // add an empty doc with metadata only
      setMetadata(inputDoc, "parsing", "no_data");
      docs.add(inputDoc);
      if (reporter != null)
        reporter.getCounter("TIKA", "DOC-NO_DATA").increment(1);
    }

    return (BehemothDocument[])docs.toArray(new BehemothDocument[0]);
  }
  
  private static void setMetadata(BehemothDocument doc, String name, String value) {
    if (doc.getMetadata() == null) {
      doc.setMetadata(new MapWritable());
    }
    if (value == null) {
      value = "";
    }
    doc.getMetadata().put(new Text(name), new Text(value));
  }
  
  private static void addMetadata(BehemothDocument doc, String name, String value) {
    if (doc.getMetadata() == null) {
      doc.setMetadata(new MapWritable());
    }
    Text key = new Text(name);
    Text oldValue = (Text)doc.getMetadata().get(key);
    if (oldValue != null) {
      if (value == null) {
        return;
      }
      Text newValue = new Text(oldValue.toString() + "," + value);
      doc.getMetadata().put(key,  newValue);
    } else {
      if (value == null) {
        value = "";
      }
      doc.getMetadata().put(key, new Text(value));
    }
  }

  // ErrorParser is used as a fallback parser to always produce an error on
  // unknown / unparseable content.
  private static class ErrorParser implements Parser {
    static final ErrorParser INSTANCE = new ErrorParser();

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
      return Collections.singleton(new MediaType("application", "octet-stream"));
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {
      throw new TikaException("unsupported mimeType " + metadata.get(Metadata.CONTENT_TYPE));
    }
    
  }
  
  private static class CollectingParser extends ParserDecorator {
    private static final long serialVersionUID = 1508171654130339149L;
    
    private static final int maxStackDepth = 20;
    
    List<BehemothDocument> docs;
    Stack<String> nested = new Stack<String>();
    Metadata parentMeta;
    String uniqueKey;
    String parentUrl;
    String batchId;
    int count = 0;
    boolean images, flatten, addFailedDocs, original;
    ParseContext plainContext = new ParseContext();
    
    public CollectingParser(AutoDetectParser parser,
            List<BehemothDocument> docs, Metadata parentMeta, String batchId,
            String uniqueKey, String parentUrl, boolean images,
            boolean flatten, boolean original, boolean addFailedDocs) {
      super(parser);
      this.docs = docs;
      this.uniqueKey = uniqueKey;
      this.parentUrl = parentUrl;
      this.batchId = batchId;
      this.parentMeta = parentMeta;
      this.images = images;
      this.flatten = flatten;
      this.original = original;
      this.addFailedDocs = addFailedDocs;
      this.plainContext.set(AutoDetectParser.class, parser);
    }

    @Override
    public void parse(InputStream stream, ContentHandler dummy,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {
      // XXX
      String resourceName = metadata.get(Metadata.RESOURCE_NAME_KEY);
      //log.info("ENTER:\n " + resourceName + " " + dummy.getClass().getName());
      //log.info(" " + metadata);
      StringBuilder thisUrl = new StringBuilder();
      thisUrl.append(parentUrl);
      StringBuilder localName = new StringBuilder();
      StringBuilder containerName = null;
      if (!nested.isEmpty()) {
        if (resourceName == null) {
          resourceName = "item" + count++;
        }
        for (int i = 1; i < nested.size(); i++) {
          if (i > 1) {
            localName.append(':');
          }
          localName.append(nested.get(i));
        }
        containerName = new StringBuilder(parentUrl);
        if (localName.length() > 0) {
          containerName.append(':');
          containerName.append(localName);
          localName.append(':');
        }
        localName.append(resourceName);
        thisUrl.append(':');
        thisUrl.append(localName);
      } else {
        localName.append(resourceName);
      }
      if (nested.size() > maxStackDepth) {
        LOG.warn("Avoiding parsing loop in " + thisUrl + ", nesting level: " + nested.size());
        return;
      }
      String contentType = metadata.get(Metadata.CONTENT_TYPE);
      if (contentType != null && contentType.startsWith("image") && !images) {
        //log.info("CP skip " + thisUrl + " type " + contentType);
        return;
      }
      nested.push(resourceName);
      try {
        //log.info("CP handle " + thisUrl);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Metadata m = new Metadata();
        // silly, should be a clone or smth.
        for (String name : metadata.names()) {
          String[] vals = metadata.getValues(name);
          //System.err.println(name + " -> " + Arrays.toString(vals));
          for (String v : vals) {
            m.add(name, v);
          }
        }
        // either append to the parent doc or create a sub-doc
        // pity we have to do this twice...
        AutoDetectParser p = (AutoDetectParser)getWrappedParser();
        boolean archive = false;
        Detector d = p.getDetector();
        MediaType type = d.detect(stream, m);
        //log.info("MEDIA_TYPE: " + type);
        if (type != null) {
          Parser subParser = p.getParsers(context).get(type);
          if (subParser != null) {
            //log.info("-subParser: " + subParser.getClass().getName());
            if (subParser instanceof DefaultParser) {
              Parser subSub = ((DefaultParser)subParser).getParsers().get(type);
              //log.info("-subSubParser: " + subSub.getClass().getName());
              if (subSub != null && subSub.getClass().getName().equals("org.apache.tika.parser.pkg.PackageParser")) {
                archive = true;
              }
            } else if (subParser.getClass().getName().equals("org.apache.tika.parser.pkg.PackageParser")) {
              archive = true;
            }
          }
        }
        if (type.toString().equals("application/java-archive")) { // hack to recognize jars
          archive = true;
        }
        ContentHandler handler = new LucidBodyContentHandler(out, defaultOutputEncoding);
        // buffer if the original content is needed
        ByteArrayOutputStream cache = null;
        if (original) {
          cache = new ByteArrayOutputStream();
          TeeInputStream tee = new TeeInputStream(stream, cache);
          stream = tee;
        }
        if (archive) {
          super.parse(stream, handler, m, context);
        } else {
          if (flatten) {
            p.parse(stream, handler, m, plainContext);
          } else {
            super.parse(stream, handler, m, context);            
          }
        }
        BehemothDocument doc = new BehemothDocument();
        String mime = null;
        String[] mimes = m.getValues("Content-Type");
        m.remove("Content-Type");
        if (mimes != null && mimes.length > 0) {
          for (String mn : mimes) {
            if (mn.equals(contentType)) { // skip
              continue;
            } else {
              mime = mn;
              break;
            }
          }
        }
        if (mime == null) {
          mime = contentType;
        }
        if (mime.startsWith("image") && !images) {
          //System.err.println("CP skip " + thisUrl + " " + mime);
          return;
        }
        m.set("Content-Type", mime);
        byte[] charData = out.toByteArray();
        // TODO: Is the data really necessarily UTF-8?
        String body = new String(charData, "UTF-8");
        LOG.trace("---\nExtracted content body length: " + body.length() + " extracted content body: <<" + body + ">>");
  
        // Add body field to generated document.
        doc.setText(body);
        if (containerName != null) {
          addMetadata(doc, CONTAINER_FIELD, containerName.toString());
        }
        if (resourceName != null) {
          addMetadata(doc, RESOURCE_FIELD, resourceName);
        }
  
        // add raw content if requested
        // XXX not supported for now?
//        if (original) {
//          doc.addField(FieldMapping.ORIGINAL_CONTENT, cache.toByteArray());
//        }
        // Add the id field, if not already set
        // Note: Key will frequently be a URL or at least approximate a URL
        // in appearance, but is not technically required to be a 100% valid URL.
        if (!doc.getMetadata().containsKey(new Text(uniqueKey))) {
          setMetadata(doc, uniqueKey, thisUrl.toString());
        }
  
        // TODO: When/where/how do the control fields get added?
        
        addMetadata(doc, CRAWL_URI_FIELD, thisUrl.toString());
  
        // Optionally add the "batch_id" field
        addMetadata(doc, BATCH_ID_FIELD, batchId);
        // copy metadata
        for (String name : m.names()) {
          String[] fieldValues = m.getValues(name);
          for (String value : fieldValues) {
            addMetadata(doc, name, value);
          }
        }
        // add parentMeta if doesn't exist
        for (String name : parentMeta.names()) {
          if (doc.getMetadata().containsKey(new Text(name))) {
            continue;
          }
          for (String v : parentMeta.getValues(name)) {
            addMetadata(doc, name, v);
          }
        }
        docs.add(doc);
        //System.err.println("LEAVE " + resourceName + " nested: " + nested);
      } catch (Throwable e) {
        if (addFailedDocs) {
          BehemothDocument doc = new BehemothDocument();
          setMetadata(doc, uniqueKey, thisUrl.toString());
          for (String m : metadata.names()) {
            String[] vals = metadata.getValues(m);
            for (String v : vals) {
              addMetadata(doc, m, v);
            }
          }
          StringWriter sw = new StringWriter();
          PrintWriter pw = new PrintWriter(sw);
          e.printStackTrace(pw);
          pw.flush();
          setMetadata(doc, "parsing", "failed: (invalid format?) " + sw.toString());
          docs.add(doc);
        } else {
          LOG.warn("Parsing " + thisUrl + " failed: " + e.getMessage());
          return;
        }
      } finally {
        nested.pop();
      }
    }
    
  }
}
