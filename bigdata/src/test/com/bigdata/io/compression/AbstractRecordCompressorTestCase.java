/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on May 2, 2009
 */

package com.bigdata.io.compression;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase2;

import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.SerializerUtil;

/**
 * Abstract base class for {@link IRecordCompressor} test suites.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME add unit tests using read-only {@link ByteBuffer}s and one
 *          using a {@link DirectBufferPool} to give us a direct buffer for
 *          testing. This will test the logic that handles cases when we can not
 *          access the backing byte[].
 */
public abstract class AbstractRecordCompressorTestCase extends TestCase2
        implements IRecordCompressorFactory {

    /**
     * 
     */
    public AbstractRecordCompressorTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRecordCompressorTestCase(String name) {
        super(name);
    }

    /**
     * A bunch of words derived from a stopwords list that are used to generate
     * random strings comprised of a redundant set of terms. This approach was
     * chosen in order to give the compression algorithm some realistic data on
     * which to work.
     */
    private static String[] words = new String[] {
        "a", 
        "a's", 
        "able", 
        "about", 
        "above", 
        "according", 
        "accordingly", 
        "across", 
        "actually", 
        "after", 
        "afterwards", 
        "again", 
        "against", 
        "ain't", 
        "all", 
        "allow", 
        "allows", 
        "almost", 
        "alone", 
        "along", 
        "already", 
        "also", 
        "although", 
        "always", 
        "am", 
        "among", 
        "amongst", 
        "an", 
        "and", 
        "another", 
        "any", 
        "anybody", 
        "anyhow", 
        "anyone", 
        "anything", 
        "anyway", 
        "anyways", 
        "anywhere", 
        "apart", 
        "appear", 
        "appreciate", 
        "appropriate", 
        "are", 
        "aren't", 
        "around", 
        "as", 
        "aside", 
        "ask", 
        "asking", 
        "associated", 
        "at", 
        "available", 
        "away", 
        "awfully", 
        "b", 
        "be", 
        "became", 
        "because", 
        "become", 
        "becomes", 
        "becoming", 
        "been", 
        "before", 
        "beforehand", 
        "behind", 
        "being", 
        "believe", 
        "below", 
        "beside", 
        "besides", 
        "best", 
        "better", 
        "between", 
        "beyond", 
        "both", 
        "brief", 
        "but", 
        "by", 
        "c", 
        "c'mon", 
        "c's", 
        "came", 
        "can", 
        "can't", 
        "cannot", 
        "cant", 
        "cause", 
        "causes", 
        "certain", 
        "certainly", 
        "changes", 
        "clearly", 
        "co", 
        "com", 
        "come", 
        "comes", 
        "concerning", 
        "consequently", 
        "consider", 
        "considering", 
        "contain", 
        "containing", 
        "contains", 
        "corresponding", 
        "could", 
        "couldn't", 
        "course", 
        "currently", 
        "d", 
        "definitely", 
        "described", 
        "despite", 
        "did", 
        "didn't", 
        "different", 
        "do", 
        "does", 
        "doesn't", 
        "doing", 
        "don't", 
        "done", 
        "down", 
        "downwards", 
        "during", 
        "e", 
        "each", 
        "edu", 
        "eg", 
        "eight", 
        "either", 
        "else", 
        "elsewhere", 
        "enough", 
        "entirely", 
        "especially", 
        "et", 
        "etc", 
        "even", 
        "ever", 
        "every", 
        "everybody", 
        "everyone", 
        "everything", 
        "everywhere", 
        "ex", 
        "exactly", 
        "example", 
        "except", 
        "f", 
        "far", 
        "few", 
        "fifth", 
        "first", 
        "five", 
        "followed", 
        "following", 
        "follows", 
        "for", 
        "former", 
        "formerly", 
        "forth", 
        "four", 
        "from", 
        "further", 
        "furthermore", 
        "g", 
        "get", 
        "gets", 
        "getting", 
        "given", 
        "gives", 
        "go", 
        "goes", 
        "going", 
        "gone", 
        "got", 
        "gotten", 
        "greetings", 
        "h", 
        "had", 
        "hadn't", 
        "happens", 
        "hardly", 
        "has", 
        "hasn't", 
        "have", 
        "haven't", 
        "having", 
        "he", 
        "he's", 
        "hello", 
        "help", 
        "hence", 
        "her", 
        "here", 
        "here's", 
        "hereafter", 
        "hereby", 
        "herein", 
        "hereupon", 
        "hers", 
        "herself", 
        "hi", 
        "him", 
        "himself", 
        "his", 
        "hither", 
        "hopefully", 
        "how", 
        "howbeit", 
        "however", 
        "i", 
        "i'd", 
        "i'll", 
        "i'm", 
        "i've", 
        "ie", 
        "if", 
        "ignored", 
        "immediate", 
        "in", 
        "inasmuch", 
        "inc", 
        "indeed", 
        "indicate", 
        "indicated", 
        "indicates", 
        "inner", 
        "insofar", 
        "instead", 
        "into", 
        "inward", 
        "is", 
        "isn't", 
        "it", 
        "it'd", 
        "it'll", 
        "it's", 
        "its", 
        "itself", 
        "j", 
        "just", 
        "k", 
        "keep", 
        "keeps", 
        "kept", 
        "know", 
        "knows", 
        "known", 
        "l", 
        "last", 
        "lately", 
        "later", 
        "latter", 
        "latterly", 
        "least", 
        "less", 
        "lest", 
        "let", 
        "let's", 
        "like", 
        "liked", 
        "likely", 
        "little", 
        "look", 
        "looking", 
        "looks", 
        "ltd", 
        "m", 
        "mainly", 
        "many", 
        "may", 
        "maybe", 
        "me", 
        "mean", 
        "meanwhile", 
        "merely", 
        "might", 
        "more", 
        "moreover", 
        "most", 
        "mostly", 
        "much", 
        "must", 
        "my", 
        "myself", 
        "n", 
        "name", 
        "namely", 
        "nd", 
        "near", 
        "nearly", 
        "necessary", 
        "need", 
        "needs", 
        "neither", 
        "never", 
        "nevertheless", 
        "new", 
        "next", 
        "nine", 
        "no", 
        "nobody", 
        "non", 
        "none", 
        "noone", 
        "nor", 
        "normally", 
        "not", 
        "nothing", 
        "novel", 
        "now", 
        "nowhere", 
        "o", 
        "obviously", 
        "of", 
        "off", 
        "often", 
        "oh", 
        "ok", 
        "okay", 
        "old", 
        "on", 
        "once", 
        "one", 
        "ones", 
        "only", 
        "onto", 
        "or", 
        "other", 
        "others", 
        "otherwise", 
        "ought", 
        "our", 
        "ours", 
        "ourselves", 
        "out", 
        "outside", 
        "over", 
        "overall", 
        "own", 
        "p", 
        "particular", 
        "particularly", 
        "per", 
        "perhaps", 
        "placed", 
        "please", 
        "plus", 
        "possible", 
        "presumably", 
        "probably", 
        "provides", 
        "q", 
        "que", 
        "quite", 
        "qv", 
        "r", 
        "rather", 
        "rd", 
        "re", 
        "really", 
        "reasonably", 
        "regarding", 
        "regardless", 
        "regards", 
        "relatively", 
        "respectively", 
        "right", 
        "s", 
        "said", 
        "same", 
        "saw", 
        "say", 
        "saying", 
        "says", 
        "second", 
        "secondly", 
        "see", 
        "seeing", 
        "seem", 
        "seemed", 
        "seeming", 
        "seems", 
        "seen", 
        "self", 
        "selves", 
        "sensible", 
        "sent", 
        "serious", 
        "seriously", 
        "seven", 
        "several", 
        "shall", 
        "she", 
        "should", 
        "shouldn't", 
        "since", 
        "six", 
        "so", 
        "some", 
        "somebody", 
        "somehow", 
        "someone", 
        "something", 
        "sometime", 
        "sometimes", 
        "somewhat", 
        "somewhere", 
        "soon", 
        "sorry", 
        "specified", 
        "specify", 
        "specifying", 
        "still", 
        "sub", 
        "such", 
        "sup", 
        "sure", 
        "t", 
        "t's", 
        "take", 
        "taken", 
        "tell", 
        "tends", 
        "th", 
        "than", 
        "thank", 
        "thanks", 
        "thanx", 
        "that", 
        "that's", 
        "thats", 
        "the", 
        "their", 
        "theirs", 
        "them", 
        "themselves", 
        "then", 
        "thence", 
        "there", 
        "there's", 
        "thereafter", 
        "thereby", 
        "therefore", 
        "therein", 
        "theres", 
        "thereupon", 
        "these", 
        "they", 
        "they'd", 
        "they'll", 
        "they're", 
        "they've", 
        "think", 
        "third", 
        "this", 
        "thorough", 
        "thoroughly", 
        "those", 
        "though", 
        "three", 
        "through", 
        "throughout", 
        "thru", 
        "thus", 
        "to", 
        "together", 
        "too", 
        "took", 
        "toward", 
        "towards", 
        "tried", 
        "tries", 
        "truly", 
        "try", 
        "trying", 
        "twice", 
        "two", 
        "u", 
        "un", 
        "under", 
        "unfortunately", 
        "unless", 
        "unlikely", 
        "until", 
        "unto", 
        "up", 
        "upon", 
        "us", 
        "use", 
        "used", 
        "useful", 
        "uses", 
        "using", 
        "usually", 
        "uucp", 
        "v", 
        "value", 
        "various", 
        "very", 
        "via", 
        "viz", 
        "vs", 
        "w", 
        "want", 
        "wants", 
        "was", 
        "wasn't", 
        "way", 
        "we", 
        "we'd", 
        "we'll", 
        "we're", 
        "we've", 
        "welcome", 
        "well", 
        "went", 
        "were", 
        "weren't", 
        "what", 
        "what's", 
        "whatever", 
        "when", 
        "whence", 
        "whenever", 
        "where", 
        "where's", 
        "whereafter", 
        "whereas", 
        "whereby", 
        "wherein", 
        "whereupon", 
        "wherever", 
        "whether", 
        "which", 
        "while", 
        "whither", 
        "who", 
        "who's", 
        "whoever", 
        "whole", 
        "whom", 
        "whose", 
        "why", 
        "will", 
        "willing", 
        "wish", 
        "with", 
        "within", 
        "without", 
        "won't", 
        "wonder", 
        "would", 
        "would", 
        "wouldn't", 
        "x", 
        "y", 
        "yes", 
        "yet", 
        "you", 
        "you'd", 
        "you'll", 
        "you're", 
        "you've", 
        "your", 
        "yours", 
        "yourself", 
        "yourselves", 
        "z", 
        "zero"
        };

    final Random r = new Random();
    
    /**
     * Generate a record comprised of <i>n</i> random terms selected from
     * {@link #words}. The terms are concatenated with whitespace separators
     * and then serialized as a byte[] which is returned to the caller.
     * 
     * @param n
     *            The #of terms to include in the record.
     */
    protected byte[] getRandomRecord(int n) {
        
        StringBuilder sb = new StringBuilder();
        
        for(int i=0; i<n; i++ ) {

            if(i>0) sb.append(" ");

            sb.append(words[r.nextInt(words.length)]);
            
        }
        
        return sb.toString().getBytes();
        
    }

//    /**
//     * Test helper applies the compression algorithm to the data and then
//     * verifies that the expected data can be recovered by applying the
//     * decompression algorithm.
//     * 
//     * @param c
//     *            The (de-)compressor.
//     * @param expected
//     *            The data to be compressed.
//     * 
//     * @return The #of bytes in the compressed record.
//     */
//    protected int doCompressionTestWithByteBuffer(RecordCompressor c, final byte[] expected) {
//
//        // default size to something that will be large enough.
//        ByteArrayOutputStream baos = new ByteArrayOutputStream(expected.length);
//
//        ByteBuffer buf = ByteBuffer.wrap(expected);
//        
//        // compress the data onto the buffer.
//        c.compress(buf, baos);
//
//        // obtain a copy of the compressed data.
//        final byte[] compressed = baos.array();
//        
//        // reset the buffer to receive the results.
//        buf.clear();
//        
//        // decompress the compressed data.
//        final byte[] actual = c.decompress(compressed);
//        
//        assertEquals(expected,actual);
//        
//        return compressed.length;
//
//    }

    /**
     * Test helper applies the compression algorithm to the data and then
     * verifies that the expected data can be recovered by applying the
     * decompression algorithm.
     * 
     * @param c
     *            The (de-)compressor.
     * @param expected
     *            The data to be compressed.
     * 
     * @return The #of bytes in the compressed record.
     */
    protected int doCompressionTest(final IRecordCompressor c,
            final byte[] expected, final int off, final int len) {

        // default output buffer to something that will be large enough.
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(
                expected.length);

        {
            
            // wrap up in a ByteBuffer.
            final ByteBuffer wrapper = ByteBuffer.wrap(expected, off, len);
            assertEquals(off + len, wrapper.limit());
            assertEquals(off, wrapper.position());

            // compress the data onto the output stream.
            c.compress(wrapper, baos);

            /*
             * verify that the position() was advanced to the limit() and that
             * the limit was not changed.
             */
            assertEquals(len + off, wrapper.limit());
            assertEquals(len + off, wrapper.position());
            
        }
        
        // obtain a copy of the compressed data.
        final byte[] compressed = baos.toByteArray();

        /*
         * Decompress the compressed data onto an exact fit byte[].
         */
        final byte[] actual;
        {

            /*
             * Decompress the compressed data. This returns a view onto a shared
             * buffer. The data between position() and limit() are the decompressed
             * data.
             */
            final ByteBuffer decompressed = c.decompress(compressed);
            
            assertEquals(0, decompressed.position());
            
            assertEquals(len, decompressed.limit());
            
            assertEquals(len, decompressed.remaining());

            /*
             * Copy the decompressed data into an exact fit buffer so that we
             * can use a test helper to verify the decompressed data.
             */
            
            actual = new byte[decompressed.remaining()];
            
            decompressed.get(actual);

        }

        /*
         * Verify the decompressed data.
         */
        for (int i = off, j = 0; i < len; i++, j++) {
     
            if (expected[i] != actual[j]) {

                fail("bytes differ at offset=" + j
                        + " in decompressed data: expected.length="
                        + expected.length + ", off=" + off + ", len=" + len);
                
            }
            
        }

        return compressed.length;

    }

    /**
     * Test ability to compress and decompress data.
     */
    public void test_recordCompressor01() {

        final IRecordCompressor c = getInstance();

        final byte[] expected = getRandomRecord(10000);

        doCompressionTest(c, expected, 0, expected.length);

    }

    /**
     * Test ability to compress and decompress data using a de-serialized
     * compression provider.
     */
    public void test_recordCompressor02() {

        final IRecordCompressor c = (IRecordCompressor) SerializerUtil
                .deserialize(SerializerUtil.serialize(getInstance()));

        final byte[] expected = getRandomRecord(10000);

        doCompressionTest(c, expected, 0, expected.length);

    }

    /**
     * Test ability to compress and decompress zero-length data.
     */
    public void test_recordCompressor03() {

        final IRecordCompressor c = getInstance();

        final byte[] expected = new byte[] {};

        doCompressionTest(c, expected, 0, expected.length);

    }
    
    /**
     * Stress test ability to compress and decompress data.
     */
    public void test_recordCompressor_stressTest() {

        final long begin = System.nanoTime();
        
//        final int limit = 50000;
        final int limit = 2500;
        
        long sumBytes = 0l;
        long sumCompressed = 0l;
        
        IRecordCompressor c = getInstance();

        final byte[] expected = getRandomRecord(10000);

        for (int i = 0; i < limit; i++) {

            final int off = r.nextInt(expected.length / 2);

            final int len = r.nextInt(expected.length - off);

            sumBytes += len;

            sumCompressed += doCompressionTest(c, expected, off, len);

            if (i % 100 == 0) {
             
                // every so often, we (de-)serialize the compressor itself.
                c = (IRecordCompressor) SerializerUtil
                        .deserialize(SerializerUtil.serialize(c));
                
            }
            
        }

        final long elapsed = System.nanoTime() - begin;
        
        System.err.println("Compressed " + limit + " records totaling "
                + sumBytes + " bytes into " + sumCompressed + " bytes: ratio="
                + ((double) sumCompressed / (double) sumBytes) + ", rate="
                + (sumBytes / (double) TimeUnit.NANOSECONDS.toSeconds(elapsed))
                + " bytes/sec: " + c);

    }

}
