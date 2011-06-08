package com.digitalpebble.behemoth.util.archive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;

import org.apache.commons.io.IOUtils;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import com.digitalpebble.behemoth.BehemothDocument;

/**
 * Reads tar, tgz and bz archives and returns BehemothDocuments.
 */
class TarReader implements Enumeration<BehemothDocument> {

    TarEntry nextEntry = null;

    TarInputStream inputStream = null;

    /**
     * Constructor.
     * 
     * @param file
     *            The tar file.
     * @throws IOException
     *             Thrown if the file cannot be opened.
     */
    TarReader(File file) throws IOException {
        if (file.exists()) {
            inputStream = new TarInputStream(new FileInputStream(file));
        } else {
            throw new IOException("File " + file.getName() + " does not exist");
        }
    }

    /**
     * Constructor.
     */
    TarReader() {
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
        TarEntry entry = nextEntry;
        nextEntry = null;
        String URI = entry.getName();
        try {
            return DocumentReader.readDocument(inputStream,
                    (int) entry.getSize(), URI);
        } catch (IOException ie) {
            IOUtils.closeQuietly(inputStream);
            return null;
        }
    }
}
