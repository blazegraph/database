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

package com.bigdata.btree.raba.codec;

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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @deprecated The {@link Deflater} does not provide access to the dictionary.
 *             For the B+Tree, what we generally want is to use a canonical
 *             huffman code since it is both more space efficient and preserves
 *             the alphabetic ordering of the coded values.
 *             <p>
 *             This test suite might be refactored for store level record
 *             compression.
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
