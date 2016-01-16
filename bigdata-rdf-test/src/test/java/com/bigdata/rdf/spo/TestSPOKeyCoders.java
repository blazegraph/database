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
 * Created on Aug 22, 2007
 */

package com.bigdata.rdf.spo;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
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
import com.bigdata.rdf.internal.IV;
import com.bigdata.test.MockTermIdFactory;

/**
 * Test suite for approaches to key compression for statement indices (keys are
 * permutations on SPOC, logically comprised of long[4] and encoded as byte[]),
 * the terms index (key is byte[] encoding the URI, literal, or bnode ID), or
 * the ids index (key is byte[] encoding a long term identifier). Key
 * compression can be used (a) before sorting the data; (b) when serializing the
 * data for a remote operation on a data service; and (c) in the nodes and
 * leaves of the indices themselves.
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

    private MockTermIdFactory factory;
    
    protected void setUp() throws Exception {

        super.setUp();
        
        factory = new MockTermIdFactory();
        
    }

    protected void tearDown() throws Exception {

        super.tearDown();

        factory = null;
        
    }
  
    /**
     * Random non-SID {@link IV}.
     */
    protected IV<?,?> getTermId() {

        /*
         * FIXME This does not exercise the case with SIDs support enabled.
         */
        return factory.newTermIdNoSids();
        
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
        
//        final IKeyBuilder keyBuilder = new KeyBuilder();
        
        final SPO[] a = new SPO[n];

        for (int i = 0; i < n; i++) {

            /*
             * Note: Only the {s,p,o} are assigned. The statement type and the
             * statement identifier are not part of the key for the statement
             * indices.
             */
            a[i] = new SPO(getTermId(), getTermId(), getTermId());
            
//            final SPO spo = a[i];
//            
//            final byte[] key = SPOKeyOrder.SPO.encodeKey(keyBuilder, spo);
//            
//            final SPO decoded = SPOKeyOrder.SPO.decodeKey(key);
//            
//            assertEquals(spo, decoded);
            
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
         * 
         * FIXME This does not exercise the case with SIDs support enabled.
         */
        final SPOTupleSerializer tupleSer = 
        	new SPOTupleSerializer(SPOKeyOrder.SPO, false/* sids */);
        
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
