package com.digitalpebble.behemoth.util.archive;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Reads tar, tgz, bzip and zip archives and returns BehemothDocuments. 
 */
public class ArchiveReader implements Enumeration<BehemothDocument> {

	Enumeration<BehemothDocument> reader = null;
	
	public ArchiveReader(File file) throws IOException {
		String fileLowerCase = file.getName().toLowerCase();
		if (fileLowerCase.endsWith(".tar.bz2")
				|| fileLowerCase.endsWith(".tbz")
				|| fileLowerCase.endsWith("tb2")) {
			reader = new BZTarReader(file);
		} else if (fileLowerCase.endsWith(".tar")) {
			reader = new TarReader(file);
		} else if (fileLowerCase.endsWith(".tgz")
				|| fileLowerCase.endsWith(".tar.gz")) {
			reader = new GZTarReader(file);
		} else if (fileLowerCase.endsWith(".zip")) {
			reader = new ZipReader(file);
		} else {
			throw new IOException("Unsupported archive type");
		}
	}

	@Override
	public boolean hasMoreElements() {
		return reader.hasMoreElements();
	}

	@Override
	public BehemothDocument nextElement() {
		return reader.nextElement();
	}
}
