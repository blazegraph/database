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
 * Created on May 26, 2011
 */

package com.bigdata.rdf.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.util.BytesUtil.UnsignedByteArrayComparator;

/**
 * Test suite for {@link IVUnicode}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIVUnicode extends TestCase2 {

    public TestIVUnicode() {
    }

    public TestIVUnicode(String name) {
        super(name);
    }

    public void test_encodeDecode1() throws IOException {

        doEncodeDecodeTest("bigdata");

    }

    public void test_encodeDecode1_emptyString() throws IOException {

        doEncodeDecodeTest("");

    }

    public void test_encodeDecode1_largeString() throws IOException {

        final int len = Short.MAX_VALUE;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }

        final String s = sb.toString();

        if (log.isInfoEnabled())
            log.info("length(s)=" + s.length());

        doEncodeDecodeTest(s);

    }

//    Note: Maximum of ~32k characters.  Practical maximum of ~ 512.
//    public void test_encodeDecode1_veryLargeString() throws IOException {
//
//        final int len = 1024000;
//
//        final StringBuilder sb = new StringBuilder(len);
//
//        for (int i = 0; i < len; i++) {
//
//            sb.append(Character.toChars('A' + (i % 26)));
//
//        }
//
//        final String s = sb.toString();
//
//        if (log.isInfoEnabled())
//            log.info("length(s)=" + s.length());
//
//        doEncodeDecodeTest(s);
//
//    }

    /**
     * Test helper for encode and decode of a single {@link String}.
     */
    private void doEncodeDecodeTest(final String expected) throws IOException {

//        // buffer for encode result.
//        final DataOutputBuffer out = new DataOutputBuffer();
//
//        // tmp buffer for encode.
//        final ByteArrayBuffer tmp = new ByteArrayBuffer();
//
//        // encode
//        final int nencoded = IVUnicode.encode(expected, out, tmp);
//
//        // extract encoded byte[].
//        final byte[] data = out.toByteArray();

        // verify encode1() gives same result.
        final byte[] data = IVUnicode.encode1(expected);
//        assertEquals(data, data1);

        // buffer for decode result
        final StringBuilder sb = new StringBuilder();
        final int ndecoded = IVUnicode.decode(new DataInputBuffer(data), sb);

        // verify the decode result.
        assertEquals(expected, sb.toString());

        // verify the #of decoded bytes.
        assertEquals(data.length, ndecoded);

    }

    public void test_encodeDecode2() throws IOException {

        doEncodeDecodeTest(new String[] { //
        "en", "bigdata" //
        });

    }

    private void doEncodeDecodeTest(final String expected[]) throws IOException {

        // buffer for encode result.
        final DataOutputBuffer out = new DataOutputBuffer();

//        // tmp buffer for encode.
//        final ByteArrayBuffer tmp = new ByteArrayBuffer();

        // the expected data in a single buffer.
        final StringBuilder exp = new StringBuilder();
        int nencoded = 0;
        for (String s : expected) {
            // encode
            final byte[] a = IVUnicode.encode1(s);
            nencoded += a.length;
            out.append(a);
            // concatenate
            exp.append(s);
        }

        // extract encoded byte[].
        final byte[] data = out.toByteArray();

        // buffer for decode result
        final StringBuilder sb = new StringBuilder();

        // decode each string component.
        final DataInputBuffer in = new DataInputBuffer(data);
        int ndecoded = 0;
        for (int i = 0; i < expected.length; i++) {

            ndecoded += IVUnicode.decode(in, sb);

        }

        // verify the decode.
        assertEquals(exp.toString(), sb.toString());

        // verify the #of decoded bytes.
        assertEquals(nencoded, ndecoded);

    }

    public void test_stress() throws IOException {

        final int ntrials = 100000;

        doTest(ntrials);
        
    }
    
    private void doTest(final int ntrials) throws IOException {

        final long begin = System.currentTimeMillis();
        
        final Random r = new Random();

        // The source data to be encoded.
        final StringBuilder sb = new StringBuilder();
        
//        // The buffer onto which we encode that data. 
//        final DataOutputBuffer outEncoded = new DataOutputBuffer();

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

//                // reset the output buffer.
//                outEncoded.reset();
                
                // encode the data.
                a = IVUnicode.encode1(expected);
                nencoded = a.length;

//                 the encoded data.
//                a = outEncoded.toByteArray();
                
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
                ndecoded = IVUnicode.decode(new ByteArrayInputStream(a, 0/* off */,
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

    /**
     * IVs must be able to report their correct mutual order. This test verifies
     * that the encoded Unicode representation has the same natural order as
     * Java {@link String}.
     */
    public void test_ordering() throws IOException {
     
        final long begin = System.currentTimeMillis();
        
        final Random r = new Random();

        final int ntrials = 10;
        
        // The source data to be encoded.
        final StringBuilder sb = new StringBuilder();

        // The buffer onto which the decoded data are written.
        final StringBuilder outDecoded = new StringBuilder();

        long nwords = 0L; // #of encoded words.
        long nchars = 0L; // #of encoded characters.
        long nbytes = 0L; // #of bytes for those encoded characters.
        
        // An array of randomly generated strings.
        final String[] values = new String[ntrials];
        
        // An array of the keys for those strings.
        final byte[][] keys = new byte[ntrials][];
        
        for (int trial = 0; trial < ntrials; trial++) {

            final int wordLength = r.nextInt(5) + 1;

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
            final byte[] a = IVUnicode.encode1(expected);
            final int nencoded = a.length;

            nwords += wordLength;
            nchars += expected.length();
            nbytes += a.length;

            values[trial] = expected;
            keys[trial] = a;

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
                ndecoded = IVUnicode.decode(new ByteArrayInputStream(a, 0/* off */,
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
        
        /*
         * Sort the String values and the byte[][] keys. If they have the same
         * natural order then the two sorted arrays will remain correlated. We
         * verify this by encode/decode cross checks.
         */
        {        
        
            Arrays.sort(values, IVUnicode.IVUnicodeComparator.INSTANCE);

            Arrays.sort(keys,UnsignedByteArrayComparator.INSTANCE);
            
            for(int i=0; i<values.length; i++) {
                
                final String expected = values[i];
                
                final String actual;
                final int ndecoded; // #of decoded bytes.
                {

                    final byte[] a = keys[i];
                    
                    // reset the output buffer.
                    outDecoded.setLength(0);

                    // decode.
                    ndecoded = IVUnicode.decode(new ByteArrayInputStream(a, 0/* off */,
                            a.length/* len */), outDecoded);

                    // extract the decoded string.
                    actual = outDecoded.toString();

                }
                
                if (!expected.equals(actual)) {
                    assertEquals("index=" + i + " of " + ntrials, expected,
                            actual);
                }

//                assertEquals(expected, expected.length(), ndecoded);
                
            }
            
        }
        
    }
    
}
