/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on May 24, 2007
 */

package com.bigdata.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import junit.framework.TestCase2;

/**
 * 
 * @todo explore adaptive vs non-adaptive huffman encoding.
 * 
 * @todo Explore huffman (Optimal alphabetic binary trees (Hu-Tucker coding and
 *       the canonical Huffman code) encoding variants that preserve the lexical
 *       ordering of the symbols. This is extremely interesting since, if the
 *       search key and the keys in the index are both encoded by the same
 *       algorithm and dictionary then it should be possible to search using the
 *       lexical order preserving huffman codes directly. In order to be
 *       compact, the scope of the dictionary needs to be limited, e.g., to a
 *       node or leaf or possibly to an index partition (or to a btree or index
 *       segment). The basic requirements are that the mapping is static based
 *       on the frequency data found in the keys. This may require an initial
 *       pass over the keys in the node. An ordered preserving code can then be
 *       built, the dictionary serialized, and the keys serialized using that
 *       code. Search would require encoding the probe key using the dictionary.
 *       If the dictionary is per-node, then the key gets encoded for each node
 *       that is searched. It may or may not be worth while to strip off common
 *       prefixes before computing the code.
 *       <p>
 *       Hu-Tucker links:
 *       <ul>
 *       <li></li>
 *       <li>http://www.google.com/search?hl=en&q=Hu-Tucker - google search</li>
 *       <li>http://www.cs.rit.edu/~std3246/thesis/node10.html - detailed
 *       online description</li>
 *       <li>T. C. Hu. Combinatorial Algorithms. Addison-Wesley Publishing Co.,
 *       1982.</li>
 *       <li>Donald E. Knuth. The Art of Computer Programming, volume 3.
 *       Addison-Wesley Publishing Co., 1973. Sorting and Searching. </li>
 *       <li>http://portal.acm.org/citation.cfm?doid=355602.361319 - published
 *       algorithm</li>
 *       <li>http://www.cs.rit.edu/~std3246/thesis/thesis.html - thesis
 *       studying the Hu-Tucker algorithm, implementation strategies, and their
 *       performance.</li>
 *       <li>http://www.cse.ucsd.edu/classes/sp07/cse202/lec9.pdf - from a
 *       lecture.</li>
 *       <li>http://www.cs.utexas.edu/users/plaxton/c/337/projects/1/desc.pdf -
 *       a class project that describes phase I of the Hu-Tucker algorithm.</li>
 *       <li>Towards a Dynamic Optimal Binary Alphabetic Tree -
 *       ftp://dimacs.rutgers.edu/pub/dimacs/TechnicalReports/TechReports/1999/99-22.ps.gz</li>
 *       <li>Describes an approach to fast sorting (word RAM) using a linear
 *       time algorithm as an alternative to Hu-Tucker.</li>
 *       </ul>
 * 
 * @see http://en.wikipedia.org/wiki/Huffman_coding
 * 
 * @see http://www.zlib.net/ (BSD style license; the Java classes are JNI
 *      wrappers that expose part of this functionality. The JNI interface could
 *      doubtless be extended to gain greater access to the underlying library.
 *      madler@alumni.caltech.edu is the remaining active author.)
 * 
 * @see http://www.jcraft.com/jzlib/ (BSD style license; this is a pure Java
 *      port of the zlib library and might provide access to the dictionary).
 * 
 * @see http://www.oberhumer.com/opensource/lzo/ (GPL license; there is a java
 *      binding and it provides faster compression and extremely fast
 *      decompression. There is a pure Java decompression package, but the
 *      compression package is only available in C. The Java code is also GPL.
 *      The author offers the possiblity of alternative licensing on request.)
 * 
 * @see http://www.faqs.org/faqs/compression-faq/ (FAQs on the compression news
 *      group).
 * 
 * @see http://www.compressconsult.com/huffman/ (Practical Huffman coding by a
 *      data compression consultant : michael@compressconsult.com).
 * 
 * @see http://www.cs.helsinki.fi/u/jikorhon/ngp/compression.html
 * 
 * @see http://coding.derkeiler.com/Archive/Java/comp.lang.java.programmer/2003-10/1545.html
 * 
 * @see http://java.sun.com/j2se/1.4.2/docs/api/java/util/zip/package-summary.html#package_description
 * 
 * @see http://www.isi.edu/in-notes/rfc1951.txt (RFC)
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestHuffmanEncoder extends TestCase2 {

    /**
     * 
     */
    public TestHuffmanEncoder() {
    }

    /**
     * @param arg0
     */
    public TestHuffmanEncoder(String arg0) {
        super(arg0);
    }

    static NumberFormat fpf;
    
    static {

        fpf = NumberFormat.getNumberInstance();

        fpf.setGroupingUsed(false);

        fpf.setMaximumFractionDigits(2);

    }

    /**
     * Simple application of the {@link Deflater} to encode a byte[] using a
     * Huffman encoding and decode that byte[].
     * 
     * @throws IOException
     */
    public void test_huffman() throws IOException {
        
        final String msg = "this is an example of huffman encoding this is an example of huffman encoding";

        final byte[] uncompressed = msg.getBytes("UTF-8");
        
        System.err.println("uncompressed(" + uncompressed.length + "): "
                + Arrays.toString(uncompressed));
        
        final byte[] compressed, decompressed;
        
        {
            
            // reuse - what state is maintained?  reset?
            final Deflater deflater = new Deflater(Deflater.BEST_SPEED);
            
            deflater.setStrategy(Deflater.HUFFMAN_ONLY);
        
            ByteArrayOutputStream baos = new ByteArrayOutputStream(/*Bytes.kilobyte32*/);
        
            DeflaterOutputStream dos = new DeflaterOutputStream(baos,deflater/*,size*/);

            dos.write(uncompressed);

            dos.flush();
            
            dos.close();
            
            compressed = baos.toByteArray();
                        
            System.err.println("  compressed(" + compressed.length + "): "
                    + Arrays.toString(compressed));

            final float ratio = (float) compressed.length / uncompressed.length;

            System.err.println("ratio=" + fpf.format(ratio));

        }
        
        {
            
            Inflater inflater = new Inflater();
            
            ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
            
            // reuse an inflator?  specify the inflator?  state maintained? reset?
            InflaterInputStream iis = new InflaterInputStream(bais,inflater);
        
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            
            while(true) {
                
                int b = iis.read();
                
                if (b == -1)
                    break;
                
                baos.write(b);
                
            }
            
            decompressed = baos.toByteArray();
            
        }
        
        assertEquals("did not decompress correctly", uncompressed, decompressed);
        
    }

    /**
     * Application and reuse of a {@link Deflater} and an {@link Inflater} to
     * encode and decode byte[]s.  While instances of these classes are reused,
     * concurrency is not tested (reuse is serialized).
     */
    public void test_huffman_reuse() throws IOException {

        final String messages[] = { "this is an example of huffman encoding",
                "this is an example of huffman encoding in which the decode and the encode are reused",
                "the lazy brown dog jumped over the fence",
                "Application and reuse of a {@link Deflater} and an {@link Inflater} to encode and decode byte[]s.  While instances of these classes are reused, concurrency is not tested (reuse is serialized)."
                };

        HuffmanEncoder c = new HuffmanEncoder();
        HuffmanDecoder d = new HuffmanDecoder();
        
        for (int i = 0; i < messages.length; i++) {

            final byte[] uncompressed = messages[i].getBytes("UTF-8");

            System.err.println("uncompressed(" + uncompressed.length + "): "
                    + Arrays.toString(uncompressed));

            final byte[] compressed = c.compress(uncompressed);

            System.err.println("  compressed(" + compressed.length + "): "
                    + Arrays.toString(compressed));

            final float ratio = (float) compressed.length / uncompressed.length;

            System.err.println("ratio=" + fpf.format(ratio));

            final byte[] decompressed = d.decompress(compressed);

            assertEquals("did not decompress correctly", uncompressed,
                    decompressed);

        }

    }

    /**
     * Class provides only the huffman encoding aspect of the ZLIB compression
     * standard. Note that this class, which wraps {@link Deflater}, uses
     * adaptive huffman encoding and (a) does NOT allow you to isolate the
     * dictionary from the input; and (b) does NOT allow you to use a static
     * code based on frequency counts or the entire input.
     * <p>
     * Instances of this class are NOT thread-safe.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class HuffmanEncoder {

        final Deflater deflater;
        
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(/*Bytes.kilobyte32*/);
        
        public HuffmanEncoder() {

            deflater = new Deflater(Deflater.BEST_SPEED);
            
            deflater.setStrategy(Deflater.HUFFMAN_ONLY);

        }

        public byte[] compress(byte[] uncompressed) {
            
            return compress(uncompressed,0,uncompressed.length);
            
        }

        public byte[] compress(byte[] uncompressed, int off, int len) {
            
            /*
             * The deflater MUST be reset between invocations.
             */
            deflater.reset();
            
            /*
             * Clear the output buffer.
             */
            baos.reset();
            
            DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater/* ,size */);

            try {

                dos.write(uncompressed,off,len);

                dos.flush();

                dos.close();
                
            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }

            return baos.toByteArray();

        }
        
    }
    
    /**
     * Decoder for ZLIB.
     * <p>
     * Instances of this class are NOT thread-safe.
     * 
     * @todo This class is unable to notice the end of the compressed input and
     *       always processes all provided input. This means that a run length
     *       must be used in the serialization format before the compressed data
     *       when compression ends before the EOF.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class HuffmanDecoder {
        
        final Inflater inflater;

        public HuffmanDecoder() {

            inflater = new Inflater();
            
        }
        
        /**
         * Decompress N bytes from the input stream.
         * 
         * @param is
         *            The input stream.
         * 
         * @param nbytes
         *            The #of bytes to be decompressed.
         * 
         * @return The decompressed data.
         */
        public byte[] decompress(DataInput is, int nbytes) {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            try {

                int c = 0;
                
                while (c<nbytes) {

                    byte b = is.readByte();

                    if (b == -1)
                        break;

                    baos.write(b);
                    
                    c++;

                }

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            return decompress(baos.toByteArray());

        }
        
        /**
         * Decompress an input stream until EOF.
         * 
         * @param is The input stream.
         * 
         * @return The decompressed data.
         */
        public byte[] decompress(DataInput is) {

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            try {

                while (true) {

                    byte b = is.readByte();

                    if (b == -1)
                        break;

                    baos.write(b);

                }

            } catch (EOFException ex) {
                
                /*
                 * Ignore - this is how we notice the end of the input stream.
                 * 
                 * @todo throwing and catching an exception is too expensive to
                 * be done as the expected code path.
                 */
                
            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            return decompress(baos.toByteArray());

        }

        /**
         * Decompress an input stream until EOF.
         * 
         * @param is The input stream.
         * 
         * @return The decompressed data.
         */
        public byte[] decompress(InputStream is) {

            /*
             * The inflater MUST be reset between invocations.
             */
            inflater.reset();
            
            // reuse an inflator?  specify the inflator?  state maintained? reset?
            InflaterInputStream iis = new InflaterInputStream(is, inflater);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            try {

                while (true) {

                    int b = iis.read();

                    if (b == -1)
                        break;

                    baos.write(b);

                }

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            return baos.toByteArray();

        }

        /**
         * Decompress a byte[].
         * 
         * @param compressed The compressed data (w/ the ZLIB header).
         * 
         * @return The decompressed data.
         */
        public byte[] decompress(byte[] compressed) {
            
            return decompress(compressed,0,compressed.length);
            
        }
        
        public byte[] decompress(byte[] compressed, int off, int len) {
            
            return decompress( new ByteArrayInputStream(compressed,off,len) );
            
        }

    }

}
