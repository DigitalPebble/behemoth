/**
 * Container for a Warc Record of type "response"
 * 
 * (C) 2009 - Carnegie Mellon University
 * 
 * 1. Redistributions of this source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 * 2. The names "Lemur", "Indri", "University of Massachusetts",  
 *    "Carnegie Mellon", and "lemurproject" must not be used to 
 *    endorse or promote products derived from this software without
 *    prior written permission. To obtain permission, contact 
 *    license@lemurproject.org.
 *
 * 4. Products derived from this software may not be called "Lemur" or "Indri"
 *    nor may "Lemur" or "Indri" appear in their names without prior written
 *    permission of The Lemur Project. To obtain permission,
 *    contact license@lemurproject.org.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE LEMUR PROJECT AS PART OF THE CLUEWEB09
 * PROJECT AND OTHER CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN 
 * NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY 
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS 
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING 
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE. 
 * 
 * @author mhoy@cs.cmu.edu (Mark J. Hoy)
 */

package edu.cmu.lemurproject;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WarcHTMLResponseRecord {

    private WarcRecord warcRecord = new WarcRecord();

    private static String SINGLE_SPACE = " ";

    private static Pattern ALL_HTML_TAGS = Pattern.compile("<(.*?)>");
    private static Pattern A_HREF_PATTERN = Pattern
            .compile("[aA].+?[hH][rR][eE][fF]=['\"](.+?)['\"].*?");
    private static Pattern AREA_HREF_PATTERN = Pattern
            .compile("[aA][rR][eE][aA].+?[hH][rR][eE][fF]=['\"](.*?)['\"].*?");
    private static Pattern FRAME_SRC_PATTERN = Pattern
            .compile("[fF][rR][aA][mM][eE].+?[sS][rR][cC]=['\"](.*?)['\"].*?");
    private static Pattern IFRAME_SRC_PATTERN = Pattern
            .compile("[iI][fF][rR][aA][mM][eE].+?[sS][rR][cC]=['\"](.*?)['\"].*?");
    private static Pattern HTTP_START_PATTERN = Pattern
            .compile("^[hH][tT][tT][pP][sS]?://.*");

    // create our pattern set
    private Vector<Pattern> patternSet = new Vector<Pattern>();

    /**
     * Default constructor
     */
    public WarcHTMLResponseRecord() {
        createPatternSet();
    }

    /**
     * Copy constructor
     * 
     * @param o
     */
    public WarcHTMLResponseRecord(WarcHTMLResponseRecord o) {
        this.warcRecord.set(o.warcRecord);
        createPatternSet();
    }

    /**
     * Constructor creation from a generic WARC record
     * 
     * @param o
     */
    public WarcHTMLResponseRecord(WarcRecord o) {
        if (o.getHeaderRecordType().compareToIgnoreCase("response") == 0) {
            this.warcRecord.set(o);
        }
        createPatternSet();
    }

    private void createPatternSet() {
        patternSet.add(A_HREF_PATTERN);
        patternSet.add(AREA_HREF_PATTERN);
        patternSet.add(FRAME_SRC_PATTERN);
        patternSet.add(IFRAME_SRC_PATTERN);
    }

    public void setRecord(WarcRecord o) {
        if (o.getHeaderRecordType().compareToIgnoreCase("response") == 0) {
            this.warcRecord.set(o);
        }
    }

    public WarcRecord getRawRecord() {
        return warcRecord;
    }

    public String getTargetURI() {
        return warcRecord.getHeaderMetadataItem("WARC-Target-URI");
    }

    public String getTargetTrecID() {
        return warcRecord.getHeaderMetadataItem("WARC-TREC-ID");
    }

    private String getNormalizedContentURL(String pageURL, String contentURL) {
        String fixedContentURL = contentURL;
        try {
            // resolve any potentially relative paths to the full URL based on
            // the page
            java.net.URI baseURI = new java.net.URI(pageURL);
            // ensure that the content doesn't have query parameters - if so,
            // strip them
            int contentParamIndex = contentURL.indexOf("?");
            if (contentParamIndex > 0) {
                fixedContentURL = contentURL.substring(0, contentParamIndex);
            }
            java.net.URI resolvedURI = baseURI.resolve(fixedContentURL);
            return resolvedURI.toString();
        } catch (URISyntaxException ex) {
        } catch (java.lang.IllegalArgumentException iaEx) {
            return fixedContentURL;
        } catch (Exception gEx) {
        }
        return "";
    }

    private HashSet<String> getMatchesOutputSet(Vector<String> tagSet,
            String baseURL) {
        HashSet<String> retSet = new HashSet<String>();

        Iterator<String> vIter = tagSet.iterator();
        while (vIter.hasNext()) {
            String thisCheckPiece = vIter.next();
            Iterator<Pattern> pIter = patternSet.iterator();
            boolean hasAdded = false;
            while (!hasAdded && pIter.hasNext()) {
                Pattern thisPattern = pIter.next();
                Matcher matcher = thisPattern.matcher(thisCheckPiece);
                if (matcher.find() && (matcher.groupCount() > 0)) {
                    String thisMatch = getNormalizedContentURL(baseURL,
                            matcher.group(1));
                    if (HTTP_START_PATTERN.matcher(thisMatch).matches()) {
                        if (!retSet.contains(thisMatch)
                                && !baseURL.equals(thisMatch)) {
                            retSet.add(thisMatch);
                            hasAdded = true;
                        } // end if (!retSet.contains(thisMatch))
                    } // end if
                      // (HTTP_START_PATTERN.matcher(thisMatch).matches())
                } // end if (matcher.find() && (matcher.groupCount() > 0))
                matcher.reset();
            } // end while (!hasAdded && pIter.hasNext())
        } // end while (vIter.hasNext())

        return retSet;
    }

    /**
     * Gets a vector of normalized URLs (normalized to this target URI) of the
     * outlinks of the page
     * 
     * @return
     */
    public Vector<String> getURLOutlinks() {
        Vector<String> retVec = new Vector<String>();

        String baseURL = getTargetURI();
        if ((baseURL == null) || (baseURL.length() == 0)) {
            return retVec;
        }

        byte[] contentBytes = warcRecord.getContent();

        ByteArrayInputStream contentStream = new ByteArrayInputStream(
                contentBytes);
        BufferedReader inReader = new BufferedReader(new InputStreamReader(
                contentStream));

        // forward to the first \n\n
        try {
            boolean inHeader = true;
            String line = null;
            while (inHeader && ((line = inReader.readLine()) != null)) {
                if (line.trim().length() == 0) {
                    inHeader = false;
                }
            }

            // now we have the rest of the lines
            // read them all into a string buffer
            // to remove all new lines
            Vector<String> htmlTags = new Vector<String>();
            while ((line = inReader.readLine()) != null) {
                // get all HTML tags from the line...
                Matcher HTMLMatcher = ALL_HTML_TAGS.matcher(line);
                while (HTMLMatcher.find()) {
                    htmlTags.add(HTMLMatcher.group(1));
                }
            }

            HashSet<String> retSet = getMatchesOutputSet(htmlTags, baseURL);

            Iterator<String> oIter = retSet.iterator();
            while (oIter.hasNext()) {
                String thisValue = oIter.next();
                if (!thisValue.equals(baseURL)) {
                    retVec.add(thisValue);
                }
            }

        } catch (IOException ioEx) {
            retVec.clear();
        }

        return retVec;
    }

}
