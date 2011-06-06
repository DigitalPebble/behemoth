package com.digitalpebble.behemoth.util.archive;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.tools.tar.TarInputStream;

/** Reads bzip archives and returns BehemothDocuments. */
class BZTarReader extends TarReader {

    /**
     * Constructor.
     * 
     * @param file
     *            The tar file.
     * @throws IOException
     *             Thrown if the file cannot be opened.
     */
    BZTarReader(File file) throws IOException {
        if (file.exists()) {
            FileInputStream fis = new FileInputStream(file);
            InputStream is;
            try {
                // first, need to remove the initial "BZ" characters in the
                // stream
                fis.read(); // "B"
                fis.read(); // "Z"
                is = new CBZip2InputStream(new BufferedInputStream(fis));
            } catch (Exception rethrow) {
                throw new IllegalStateException(rethrow);
            }
            inputStream = new TarInputStream(is);
        } else {
            throw new IOException("File " + file.getName() + " does not exist");
        }
    }
}
