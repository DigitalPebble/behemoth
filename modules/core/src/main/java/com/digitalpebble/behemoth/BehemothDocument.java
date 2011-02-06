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

package com.digitalpebble.behemoth;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.digitalpebble.behemoth.util.MimeUtil;

/**
 * Implementation of a Document using Hadoop primitives
 ***/
public class BehemothDocument implements Writable {

	public BehemothDocument() {
	}

	private String url;

	private String contentType;

	/**
	 * Text representation of a document - can be null if the document is at a
	 * binary format and has not yet been converted; or if the document had
	 * multimedia content
	 **/
	private String text;

	/** Binary content from which the text can be extracted **/
	private byte[] content;

	/** Document metadata **/
	private MapWritable metadata;

	/** List holding the annotations **/
	private List<Annotation> annotations;

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	/** The binary content retrieved. */
	public byte[] getContent() {
		return content;
	}

	public void setContent(byte[] content) {
		this.content = content;
	}

	public MapWritable getMetadata() {
		return metadata;
	}

	public void setMetadata(MapWritable metadata) {
		this.metadata = metadata;
	}

	public List<Annotation> getAnnotations() {
		if (annotations == null)
			annotations = new ArrayList<Annotation>();
		return annotations;
	}

	public void setAnnotations(List<Annotation> annotations) {
		this.annotations = annotations;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		// make sure that the mime type does not contain any
		// charset info
		this.contentType = MimeUtil.cleanMimeType(contentType);
	}

	/** serialisation **/

	public final void readFields(DataInput in) throws IOException {
		url = Text.readString(in);
		content = new byte[in.readInt()];
		in.readFully(content);
		contentType = Text.readString(in);
		boolean hasText = in.readBoolean();
		if (hasText)
			text = Text.readString(in);
		else
			text = null;
		boolean hasMD = in.readBoolean();
		if (hasMD)
			metadata.readFields(in);
		else
			metadata = null;
		// read the number of annotation types
		int numTypes = in.readInt();
		ArrayList<String> types = null;
		if (numTypes > 0) {
			types = new ArrayList<String>(numTypes);
			for (int i = 0; i < numTypes; i++) {
				types.add(Text.readString(in));
			}
		}
		int numAnnots = in.readInt();
		this.annotations = new ArrayList<Annotation>(numAnnots);
		for (int i = 0; i < numAnnots; i++) {
			Annotation annot = new Annotation();
			readAnnotationFields(annot, in, types);
			this.annotations.add(annot);
		}
	}

	public void write(DataOutput out) throws IOException {
		writeCommon(out);
		writeAnnotations(out); // write annotations
	}

	public void writeCommon(DataOutput out) throws IOException {
		Text.writeString(out, url); // write url
		out.writeInt(content.length); // write content
		out.write(content);
		if (contentType != null) {
			Text.writeString(out, contentType); // write contentType
		} else {
			Text.writeString(out, "");
		}
		out.writeBoolean(text != null);
		if (text != null)
			Text.writeString(out, text); // write text
		out.writeBoolean(metadata != null);
		if (metadata != null)
			metadata.write(out); // write metadata;
	}

	private void writeAnnotations(DataOutput out) throws IOException {
		List<String> atypes = new ArrayList<String>();
		if (annotations != null) {
			// go through the annotations and check the annotation types that
			// are present
			for (int i = 0; i < annotations.size(); i++) {
				Annotation annot = annotations.get(i);
				if (atypes.contains(annot.getType()) == false)
					atypes.add(annot.getType());
				Iterator<String> featNamIter = annot.getFeatures().keySet()
						.iterator();
				while (featNamIter.hasNext()) {
					String fn = featNamIter.next();
					if (atypes.contains(fn) == false)
						atypes.add(fn);
				}
			}
		}
		out.writeInt(atypes.size());
		// write the annotation type and feature names
		// to the output
		for (String type : atypes) {
			Text.writeString(out, type);
		}
		// write annotations
		if (annotations == null)
			out.writeInt(0);
		else
			out.writeInt(annotations.size());
		if (annotations != null) {
			for (int i = 0; i < annotations.size(); i++) {
				Annotation annot = annotations.get(i);
				writeAnnotation(annot, out, atypes);
			}
		}
	}

	protected void writeAnnotation(Annotation annot, DataOutput out,
			List<String> atypes) throws IOException {
		int typePos = atypes.indexOf(annot.getType());
		IntWritable intStringPool = new IntWritable(typePos);
		intStringPool.write(out);
		WritableUtils.writeVLong(out, annot.getStart());
		WritableUtils.writeVLong(out, annot.getEnd());
		out.writeInt(annot.getFeatureNum());

		if (annot.getFeatures() != null) {
			Iterator<String> featNameIter = annot.getFeatures().keySet()
					.iterator();
			while (featNameIter.hasNext()) {
				String fname = featNameIter.next();
				int fnamePos = atypes.indexOf(fname);
				intStringPool.set(fnamePos);
				intStringPool.write(out);
				WritableUtils.writeString(out, annot.getFeatures().get(fname));
			}
		}
	}

	public void readAnnotationFields(Annotation annot, DataInput in,
			List<String> types) throws IOException {
		IntWritable posType = new IntWritable();
		posType.readFields(in);
		annot.setType(types.get(posType.get()));
		annot.setStart(WritableUtils.readVLong(in));
		annot.setEnd(WritableUtils.readVLong(in));
		HashMap<String, String> features = null;
		int numFeatures = in.readInt();
		if (numFeatures > 0)
			features = new HashMap<String, String>(numFeatures);
		for (int i = 0; i < numFeatures; i++) {
			posType.readFields(in);
			String fname = types.get(posType.get());
			String fvalue = WritableUtils.readString(in);
			features.put(fname, fvalue);
		}
		annot.setFeatures(features);
	}

	public static BehemothDocument read(DataInput in) throws IOException {
		BehemothDocument doc = new BehemothDocument();
		doc.readFields(in);
		return doc;
	}

	
	/** By default includes the binary content in the string representation **/
	public String toString() {
		return toString(true);
	}

	public String toString(boolean binaryContent) {
		StringBuffer buffer = new StringBuffer();

		buffer.append("\nurl: ").append(url);
		buffer.append("\ncontentType: ").append(contentType);
		buffer.append("\nmetadata: ").append(metadata);
		if (binaryContent){
			buffer.append("\nContent:\n");
			int maxLengthText = Math.min(200, content.length);
			buffer.append(new String(content).substring(0, maxLengthText));
		}
		// try
		// default
		// encoding
		if (this.text != null) {
			buffer.append("\nText:\n");
			int maxLengthText = Math.min(200, text.length());
			buffer.append(text.substring(0, maxLengthText));
		}
		if (annotations == null)
			return buffer.toString();
		buffer.append("\nAnnotations:\n");
		for (Annotation ann : annotations) {
			buffer.append("\t").append(ann.toString()).append("\n");
		}

		return buffer.toString();
	}

}
