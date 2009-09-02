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
 * Created on Aug 22, 2007
 */

package com.bigdata.rdf.spo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.ICodedRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.rdf.lexicon.LexiconRelation;

/**
 * Test suite for approaches to key compression for statement indices (keys are
 * permutations on SPOC, logically comprised of long[4] and encoded as byte[]),
 * the terms index (key is byte[] encoding the URI, literal, or bnode ID), or
 * the ids index (key is byte[] encoding a long term identifier). Key
 * compression can be used (a) before sorting the data; (b) when serializing the
 * data for a remote operation on a data service; and (c) in the nodes and
 * leaves of the indices themselves.
 * 
 * FIXME test w/ int64 front-coded and int64 huffman compression. These should
 * be much better for the statement indices since the logical key is long[3] or
 * long[4].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPOKeyCoders extends TestCase2 {

    /**
     * 
     */
    public TestSPOKeyCoders() {
        super();
    }

    /**
     * @param arg0
     */
    public TestSPOKeyCoders(String arg0) {
        super(arg0);
    }

    private final Random r = new Random();
    
    /**
     * Random positive integer (note that the lower two bits indicate either
     * a URI, BNode, Literal or Statement Identifier and are randomly assigned
     * along with the rest of the bits). The actual term identifiers for a
     * triple store are assigned by the {@link ICounter} for the
     * {@link LexiconRelation}'s TERM2id index.
     */
    protected long getTermId() {
        
        return r.nextInt(Integer.MAX_VALUE - 1) + 1;
        
    }
    
    /**
     * Return an array of {@link SPO}s.
     * 
     * @param n
     *            The #of elements in the array.
     *            
     * @return The array.
     */
    protected SPO[] getData(final int n) {
        
        final SPO[] a = new SPO[n];

        for (int i = 0; i < n; i++) {

            /*
             * Note: Only the {s,p,o} are assigned. The statement type and the
             * statement identifier are not part of the key for the statement
             * indices.
             */
            a[i] = new SPO(getTermId(), getTermId(), getTermId());
            
        }

        // place into sorted order.
        Arrays.sort(a, 0, a.length, SPOComparator.INSTANCE);
        
        return a;
        
    }

    public void test_simpleCoder() {

        doRoundTripTests(SimpleRabaCoder.INSTANCE);
        
    }

    public void test_frontCoder_int8() {

        doRoundTripTests(new FrontCodedRabaCoder(8/* ratio */));
        
    }

    public void test_canonicalHuffmanCoder_int8() {

        doRoundTripTests(CanonicalHuffmanRabaCoder.INSTANCE);
        
    }

    protected void doRoundTripTests(final IRabaCoder rabaCoder) {

      doRoundTripTest(getData(0), rabaCoder);
      
      doRoundTripTest(getData(1), rabaCoder);
      
      doRoundTripTest(getData(10), rabaCoder);
      
      doRoundTripTest(getData(100), rabaCoder);

      doRoundTripTest(getData(1000), rabaCoder);
      
      doRoundTripTest(getData(10000), rabaCoder);

    }
    
    /**
     * Do a round-trip test test.
     * 
     * @param a
     *            The array of {@link SPO}s.
     * @param rabaCoder
     *            The compression provider.
     * 
     * @throws IOException
     */
    protected void doRoundTripTest(final SPO[] a, final IRabaCoder rabaCoder) {

        /*
         * Generate keys from the SPOs.
         */
        final SPOTupleSerializer tupleSer = new SPOTupleSerializer(SPOKeyOrder.SPO);
        final byte[][] keys = new byte[a.length][];
        {

            for (int i = 0; i < a.length; i++) {

                keys[i] = tupleSer.serializeKey(a[i]);

            }

        }
        final IRaba expected = new ReadOnlyKeysRaba(keys);

        /*
         * Compress the keys.
         */
        final AbstractFixedByteArrayBuffer originalData = rabaCoder.encode(
                expected, new DataOutputBuffer());

        // verify we can decode the encoded data.
        {
         
            // decode.
            final ICodedRaba actual0 = rabaCoder.decode(originalData);

            // Verify encode() results in object which can decode the
            // byte[]s.
            AbstractBTreeTestCase.assertSameRaba(expected, actual0);

            // Verify decode when we build the decoder from the serialized
            // format.
            AbstractBTreeTestCase.assertSameRaba(expected, rabaCoder
                    .decode(actual0.data()));
        }

        // Verify encode with a non-zero offset for the DataOutputBuffer
        // returns a slice which has the same data.
        {

            // buffer w/ non-zero offset.
            final int off = 10;
            final DataOutputBuffer out = new DataOutputBuffer(off,
                    new byte[100 + off]);

            // encode onto that buffer.
            final AbstractFixedByteArrayBuffer slice = rabaCoder.encode(
                    expected, out);

            // verify same encoded data for the slice.
            assertEquals(originalData.toByteArray(), slice.toByteArray());
            
        }

        // Verify decode when we build the decoder from a slice with a
        // non-zero offset
        {

            final int off = 10;
            final byte[] tmp = new byte[off + originalData.len()];
            System.arraycopy(originalData.array(), originalData.off(), tmp,
                    off, originalData.len());

            // create slice
            final FixedByteArrayBuffer slice = new FixedByteArrayBuffer(
                    tmp, off, originalData.len());

            // verify same slice.
            assertEquals(originalData.toByteArray(), slice.toByteArray());

            // decode the slice.
            final IRaba actual = rabaCoder.decode(slice);
            
            // verify same raba.
            AbstractBTreeTestCase.assertSameRaba(expected, actual);
            
        }
        
    }

}
