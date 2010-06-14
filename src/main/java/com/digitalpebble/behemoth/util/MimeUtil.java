package com.digitalpebble.behemoth.util;

public final class MimeUtil {

  private static final String SEPARATOR = ";";
  
  /**
   * Cleans a {@link MimeType} name by removing out the actual {@link MimeType},
   * from a string of the form:
   * 
   * <pre>
   *      &lt;primary type&gt;/&lt;sub type&gt; ; &lt; optional params
   * </pre>
   * 
   * @param origType
   *          The original mime type string to be cleaned.
   * @return The primary type, and subtype, concatenated, e.g., the actual mime
   *         type.
   */
  public static String cleanMimeType(String origType) {
    if (origType == null)
      return null;

    // take the origType and split it on ';'
    String[] tokenizedMimeType = origType.split(SEPARATOR);
    if (tokenizedMimeType.length > 1) {
      // there was a ';' in there, take the first value
      return tokenizedMimeType[0];
    } else {
      // there wasn't a ';', so just return the orig type
      return origType;
    }
  }
  
}
