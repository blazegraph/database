/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on May 23, 2011
 */

package com.bigdata.io.compression;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.io.DataOutputBuffer;

/**
 * Unit tests for Unicode schemes:
 * <ul>
 * <li>Standard Compression for Unicode (<a
 * href="http://userguide.icu-project.org/conversion/compression" SCSU </a>)</li>
 * <li>Byte Order Compression for Unicode (<a
 * href="http://www.unicode.org/notes/tn6/"> BOCU </a>). Note that BOCU has the
 * advantage of SCSU and also maintains code point order (which is not really
 * the same collation sort keys, but does have better "natural" order than
 * SCSU).</li>
 * </ul>
 * BOCU appears to be better than SCSU for most points, but is slower. Neither
 * of these compression schemes is supported by the native JDK.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: TestUnicodeCompressor.java 4582 2011-05-31 19:12:53Z
 *          thompsonbry $
 */
public class TestUnicodeCompressor extends TestCase2 {

    /**
     * 
     */
    public TestUnicodeCompressor() {
    }

    /**
     * @param name
     */
    public TestUnicodeCompressor(String name) {
        super(name);
    }

//    /** Note: You MUST be linked with the icu4j-charset.jar for BOCU-1 support. */
//    public void test_BOCU_available() {
//        
//        Charset.forName("BOCU-1");
//        
//    }
//
//    /** Note: You MUST be linked with the icu4j-charset.jar for SCSU support. */
//    public void test_SCSU_available() {
//        
//        Charset.forName("SCSU");
//        
//    }

    /**
     * {@link NoCompressor} encoding / decoding stress test.
     */
    public void test_None() {

        doTest(new NoCompressor());

    }

//    /**
//     * BOCU-1 encoding / decoding stress test.
//     */
//    public void test_BOCU() {
//
//        doTest(new BOCU1Compressor());
//
//    }
//
//    /**
//     * SCSU encoding / decoding stress test.
//     */
//    public void test_SCSU() {
//
//        doTest(new SCSUCompressor());
//
//    }

    private void doTest(final IUnicodeCompressor c) {

        final long begin = System.currentTimeMillis();
        
        final Random r = new Random();

        final int ntrials = 100000;

        // The source data to be encoded.
        final StringBuilder sb = new StringBuilder();
        
        // The buffer onto which we encode that data. 
        final DataOutputBuffer outEncoded = new DataOutputBuffer();

        // The buffer onto which the decoded data are written.
        final StringBuilder outDecoded = new StringBuilder();

        long nwords = 0L; // #of encoded words.
        long nchars = 0L; // #of encoded characters.
        long nbytes = 0L; // #of bytes for those encoded characters.
        
        for (int trial = 0; trial < ntrials; trial++) {

            final int wordLength = r.nextInt(20) + 1;

            // reset
            sb.setLength(0);

            // build up a string of randomly selected words.
            for (int i = 0; i < wordLength; i++) {

                if (i > 0)
                    sb.append(" ");

                sb.append(words[r.nextInt(words.length)]);

            }

            final String expected = sb.toString();

            /*
             * Encode.
             */
            final byte[] a;
            final int nencoded;
            {

                // reset the output buffer.
                outEncoded.reset();
                
                // encode the data.
                nencoded = c.encode(expected, outEncoded);
                
                // the encoded data.
                a = outEncoded.toByteArray();
                
            }

            nwords += wordLength;
            nchars += expected.length();
            nbytes += a.length;

            /*
             * Note: The caller needs to know the exact length to be decoded in
             * advance with this api. This implies that we will have to buffer
             * the data before it can be copied into an IKeyBuilder. This is not
             * a problem as long as it is the only thing in the buffer, but that
             * is not true for many use cases, including serialization of a
             * BigdataValue.
             */
            final String actual;
            final int ndecoded;
            {

                // reset the output buffer.
                outDecoded.setLength(0);

                // decode.
                ndecoded = c.decode(new ByteArrayInputStream(a, 0/* off */,
                        a.length/* len */), outDecoded);

                // extract the decoded string.
                actual = outDecoded.toString();

            }

            // verify encode/decode.
            assertEquals(expected, actual);
            
            // verify #of bytes encoded / #of bytes decoded.
            assertEquals(nencoded, ndecoded);

        }

        final long elapsed = System.currentTimeMillis() - begin;

        // The compression ratio.
        final double ratio = (double) nbytes / (double) nchars;
        
        if (log.isInfoEnabled())
            log
                    .info("nwords=" + nwords + ", nchars=" + nchars
                            + ", nbytes=" + nbytes + ", bytes/char=" + ratio
                            + ", elapsed=" + elapsed);

    }

    /**
     * A bunch of words derived from a stopwords list that are used to generate
     * random strings comprised of a redundant set of terms. This approach was
     * chosen in order to give the compression algorithm some realistic data on
     * which to work.
     */
    private static String[] words = new String[] { "a", "a's", "able", "about",
            "above", "according", "accordingly", "across", "actually", "after",
            "afterwards", "again", "against", "ain't", "all", "allow",
            "allows", "almost", "alone", "along", "already", "also",
            "although", "always", "am", "among", "amongst", "an", "and",
            "another", "any", "anybody", "anyhow", "anyone", "anything",
            "anyway", "anyways", "anywhere", "apart", "appear", "appreciate",
            "appropriate", "are", "aren't", "around", "as", "aside", "ask",
            "asking", "associated", "at", "available", "away", "awfully", "b",
            "be", "became", "because", "become", "becomes", "becoming", "been",
            "before", "beforehand", "behind", "being", "believe", "below",
            "beside", "besides", "best", "better", "between", "beyond", "both",
            "brief", "but", "by", "c", "c'mon", "c's", "came", "can", "can't",
            "cannot", "cant", "cause", "causes", "certain", "certainly",
            "changes", "clearly", "co", "com", "come", "comes", "concerning",
            "consequently", "consider", "considering", "contain", "containing",
            "contains", "corresponding", "could", "couldn't", "course",
            "currently", "d", "definitely", "described", "despite", "did",
            "didn't", "different", "do", "does", "doesn't", "doing", "don't",
            "done", "down", "downwards", "during", "e", "each", "edu", "eg",
            "eight", "either", "else", "elsewhere", "enough", "entirely",
            "especially", "et", "etc", "even", "ever", "every", "everybody",
            "everyone", "everything", "everywhere", "ex", "exactly", "example",
            "except", "f", "far", "few", "fifth", "first", "five", "followed",
            "following", "follows", "for", "former", "formerly", "forth",
            "four", "from", "further", "furthermore", "g", "get", "gets",
            "getting", "given", "gives", "go", "goes", "going", "gone", "got",
            "gotten", "greetings", "h", "had", "hadn't", "happens", "hardly",
            "has", "hasn't", "have", "haven't", "having", "he", "he's",
            "hello", "help", "hence", "her", "here", "here's", "hereafter",
            "hereby", "herein", "hereupon", "hers", "herself", "hi", "him",
            "himself", "his", "hither", "hopefully", "how", "howbeit",
            "however", "i", "i'd", "i'll", "i'm", "i've", "ie", "if",
            "ignored", "immediate", "in", "inasmuch", "inc", "indeed",
            "indicate", "indicated", "indicates", "inner", "insofar",
            "instead", "into", "inward", "is", "isn't", "it", "it'd", "it'll",
            "it's", "its", "itself", "j", "just", "k", "keep", "keeps", "kept",
            "know", "knows", "known", "l", "last", "lately", "later", "latter",
            "latterly", "least", "less", "lest", "let", "let's", "like",
            "liked", "likely", "little", "look", "looking", "looks", "ltd",
            "m", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile",
            "merely", "might", "more", "moreover", "most", "mostly", "much",
            "must", "my", "myself", "n", "name", "namely", "nd", "near",
            "nearly", "necessary", "need", "needs", "neither", "never",
            "nevertheless", "new", "next", "nine", "no", "nobody", "non",
            "none", "noone", "nor", "normally", "not", "nothing", "novel",
            "now", "nowhere", "o", "obviously", "of", "off", "often", "oh",
            "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto",
            "or", "other", "others", "otherwise", "ought", "our", "ours",
            "ourselves", "out", "outside", "over", "overall", "own", "p",
            "particular", "particularly", "per", "perhaps", "placed", "please",
            "plus", "possible", "presumably", "probably", "provides", "q",
            "que", "quite", "qv", "r", "rather", "rd", "re", "really",
            "reasonably", "regarding", "regardless", "regards", "relatively",
            "respectively", "right", "s", "said", "same", "saw", "say",
            "saying", "says", "second", "secondly", "see", "seeing", "seem",
            "seemed", "seeming", "seems", "seen", "self", "selves", "sensible",
            "sent", "serious", "seriously", "seven", "several", "shall", "she",
            "should", "shouldn't", "since", "six", "so", "some", "somebody",
            "somehow", "someone", "something", "sometime", "sometimes",
            "somewhat", "somewhere", "soon", "sorry", "specified", "specify",
            "specifying", "still", "sub", "such", "sup", "sure", "t", "t's",
            "take", "taken", "tell", "tends", "th", "than", "thank", "thanks",
            "thanx", "that", "that's", "thats", "the", "their", "theirs",
            "them", "themselves", "then", "thence", "there", "there's",
            "thereafter", "thereby", "therefore", "therein", "theres",
            "thereupon", "these", "they", "they'd", "they'll", "they're",
            "they've", "think", "third", "this", "thorough", "thoroughly",
            "those", "though", "three", "through", "throughout", "thru",
            "thus", "to", "together", "too", "took", "toward", "towards",
            "tried", "tries", "truly", "try", "trying", "twice", "two", "u",
            "un", "under", "unfortunately", "unless", "unlikely", "until",
            "unto", "up", "upon", "us", "use", "used", "useful", "uses",
            "using", "usually", "uucp", "v", "value", "various", "very", "via",
            "viz", "vs", "w", "want", "wants", "was", "wasn't", "way", "we",
            "we'd", "we'll", "we're", "we've", "welcome", "well", "went",
            "were", "weren't", "what", "what's", "whatever", "when", "whence",
            "whenever", "where", "where's", "whereafter", "whereas", "whereby",
            "wherein", "whereupon", "wherever", "whether", "which", "while",
            "whither", "who", "who's", "whoever", "whole", "whom", "whose",
            "why", "will", "willing", "wish", "with", "within", "without",
            "won't", "wonder", "would", "would", "wouldn't", "x", "y", "yes",
            "yet", "you", "you'd", "you'll", "you're", "you've", "your",
            "yours", "yourself", "yourselves", "z", "zero" };

}
