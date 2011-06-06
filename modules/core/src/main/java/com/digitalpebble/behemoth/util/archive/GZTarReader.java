package com.digitalpebble.behemoth.util.archive;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.tools.tar.TarInputStream;

/** Reads gzip archives and returns BehemothDocuments. */
class GZTarReader extends TarReader {

    /**
     * Constructor.
     * 
     * @param file
     *            The tar file.
     * @param document
     *            The document to store the results in.
     * @throws IOException
     *             Thrown if the file cannot be opened.
     */
    GZTarReader(File file) throws IOException {
        if (file.exists()) {
            inputStream = new TarInputStream(new GZIPInputStream(
                    new FileInputStream(file)));
        } else {
            throw new IOException("File " + file.getName() + " does not exist");
        }
    }
}
