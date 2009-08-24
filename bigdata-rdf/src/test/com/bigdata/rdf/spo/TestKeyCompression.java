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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.ICounter;
import com.bigdata.btree.compression.IDataSerializer;
import com.bigdata.btree.compression.PrefixSerializer;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.MutableValuesRaba;
import com.bigdata.btree.raba.ReadOnlyKeysRaba;
import com.bigdata.rdf.lexicon.LexiconRelation;

/**
 * Test suite for approaches to key compression for statement indices (keys are
 * permutations on SPOC, logically comprised of long[4] and encoded as byte[]),
 * the terms index (key is byte[] encoding the URI, literal, or bnode ID), or
 * the ids index (key is byte[] encoding a long term identifier). Key
 * compression can be used (a) before sorting the data; (b) when serializing the
 * data for a remote operation on a data service; and (c) in the nodes and
 * leaves of the indices themselves.
 * <p>
 * 
 * @todo allow the client to use custom serialization for the keys and values.
 *       For example, for RDF statements inserted in sorted order a run length
 *       encoding by position would be very fast and compact:
 * 
 * <pre>
 *   [x][c][a]
 *   [x][d][b]
 *   [x][d][e]
 *   [x][f][a]
 *   
 *   would be about a 50% savings.
 *   
 *    x  c  a
 *    -  d  b
 *    -  -  e
 *    -  f  a
 *   
 *   Or
 *   
 *    4x 1c a 2d b e f a
 * </pre>
 * 
 * Since we never store identical triples, the last position always varies and
 * does not need a run length counter.
 * 
 * If we use a dictionary, then we can assign codes to term identifiers and
 * write out a code stream.
 * 
 * These two approaches can also be combined....
 * 
 * FIXME this was just a sketch of some ideas and this form of compression was
 * never implemented. There are some compression methods for RDF that have been
 * implemented and they SHOULD be tested here.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestKeyCompression extends TestCase2 {

    /**
     * 
     */
    public TestKeyCompression() {
        super();
    }

    /**
     * @param arg0
     */
    public TestKeyCompression(String arg0) {
        super(arg0);
    }

    /**
     * @todo write tests.
     */
    public void test_nothing() {
        
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
    protected SPO[] getData(int n) {
        
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

    public void test_prefixSerializer() throws IOException {
        
        final IDataSerializer ser = PrefixSerializer.INSTANCE;
        
        doRoundTripTest(getData(0), ser);
        
        doRoundTripTest(getData(1), ser);
        
        doRoundTripTest(getData(10), ser);
        
        doRoundTripTest(getData(100), ser);

        doRoundTripTest(getData(1000), ser);
        
        doRoundTripTest(getData(10000), ser);

    }
    
//    public void test_fastKeyCompression() throws IOException {
//
//        final IDataSerializer ser = FastRDFKeyCompression.N3;
//        
//        doRoundTripTest(getData(0), ser);
//        
//        doRoundTripTest(getData(1), ser);
//        
//        doRoundTripTest(getData(10), ser);
//        
//        doRoundTripTest(getData(100), ser);
//
//        doRoundTripTest(getData(1000), ser);
//        
//        doRoundTripTest(getData(10000), ser);
//
//    }
    
    /**
     * Do a round-trip compression test.
     * 
     * @param a
     *            The array of {@link SPO}s.
     * @param ser
     *            The compression provider.
     * 
     * @throws IOException
     */
    protected void doRoundTripTest(final SPO[] a, final IDataSerializer ser)
            throws IOException {

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

        /*
         * Compress the keys.
         */
        final byte[] data;
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final DataOutputStream os = new DataOutputStream(baos);

            final IRaba raba = new ReadOnlyKeysRaba(keys);

            ser.write(os, raba);

            os.flush();
            
            data = baos.toByteArray();

            log.info("#keys="
                            + a.length
                            + ", #bytes="
                            + data.length
                            + (a.length <= 10 ? ", data="
                                    + Arrays.toString(data) : ""));
            
        }

        /*
         * De-compress the keys and compare to the keys generated above.
         */
        final IRaba raba2;
        {
            
            final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            
            final DataInputStream in = new DataInputStream(bais);
            
            raba2 = new MutableValuesRaba(0/* fromIndex */, 0/* toIndex */,
                    a.length/* capacity */, new byte[a.length][]);

            ser.read(in, raba2);
            
            in.close();

        }
        
        // verify decompressed data.
        {
            
            final Iterator<byte[]> itr = raba2.iterator();
            
            int i = 0;
            
            while(itr.hasNext()) {
                
                final byte[] expected = keys[i];
                
                final byte[] actual = itr.next();

//                System.err.println("#nkeys=" + a.length + ", spo="
//                        + a[i].toString() + ", expected="
//                        + Arrays.toString(expected) + ", actual="
//                        + Arrays.toString(actual));
                
                assertEquals("#keys=" + a.length + ", i=" + i, expected, actual);

                i++;
                
            }
            
        }
        
    }

}
