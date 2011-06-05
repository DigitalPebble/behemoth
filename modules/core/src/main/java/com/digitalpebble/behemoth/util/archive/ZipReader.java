package com.digitalpebble.behemoth.util.archive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Reads zip files and returns BehemothDocuments.
 */
class ZipReader implements Enumeration<BehemothDocument> {

	private ZipEntry nextEntry = null;

	private ZipInputStream inputStream = null;

	/**
	 * Constructor.
	 * 
	 * @param file The tar file.
	 * @throws IOException Thrown if the file cannot be opened.
	 */
	public ZipReader(File file) throws IOException {
		if (file.exists()) {
			inputStream = new ZipInputStream(new FileInputStream(file));
		} else {
			throw new IOException("File " + file.getName() + " does not exist");
		}
	}

	@Override
	public boolean hasMoreElements() {
		try {
			if (nextEntry == null) {
				do {
					nextEntry = inputStream.getNextEntry();
					if (nextEntry == null) {
						return false;
					}
				} while (nextEntry.isDirectory());
				return true;
			} else {
				return true;
			}
		} catch (IOException ie) {
			IOUtils.closeQuietly(inputStream);
			return false;
		}
	}

	@Override
	public BehemothDocument nextElement() {
		if (nextEntry == null) {
			if (!hasMoreElements())
				return null;
		}
		ZipEntry entry = nextEntry;
		nextEntry = null;
		String URI = entry.getName().toString();
		try {
			return DocumentReader.readDocument(inputStream, (int) entry.getSize(), URI);
		} catch (IOException ie) {
			IOUtils.closeQuietly(inputStream);
			return null;
		}
	}
}