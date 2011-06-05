package com.digitalpebble.behemoth.util.archive;

import java.io.IOException;
import java.io.InputStream;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Base class for archive readers.
 */
class DocumentReader {

	/**
	 * Read a BehemothDocument from an InputStream.
	 * 
	 * @param is InputStream.
	 * @param size Size of the document.
	 * @param URI The URI of the document.
	 * @return The BehemothDocument.
	 */
	static BehemothDocument readDocument(InputStream is, int size, String URI)
			throws IOException {
		byte[] fileBArray = new byte[size];
		BehemothDocument document = new BehemothDocument();
		is.read(fileBArray);
		document.setUrl(URI);
		document.setContent(fileBArray);
		return document;
	}
}
