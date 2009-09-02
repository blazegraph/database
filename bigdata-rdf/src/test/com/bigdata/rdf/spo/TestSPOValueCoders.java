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
import java.util.Random;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.ICounter;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.btree.raba.codec.ICodedRaba;
import com.bigdata.btree.raba.codec.IRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.AbstractFixedByteArrayBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.FixedByteArrayBuffer;
import com.bigdata.rdf.lexicon.ITermIdCodes;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.model.StatementEnum;

/**
 * Test suite for approaches to value compression for statement indices. The
 * values exist if either statement identifiers or truth maintenance is being
 * used, otherwise NO values are associated with the keys in the statement
 * indices. For statement identifiers, the value is the SID (int64). For truth
 * maintenance, the value is the {@link StatementEnum}. If statement identifiers
 * and truth maintenance are both enabled, then both the SID and the
 * {@link StatementEnum} are stored as the value under the key.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPOValueCoders extends TestCase2 {

    /**
     * 
     */
    public TestSPOValueCoders() {
        super();
    }

    /**
     * @param arg0
     */
    public TestSPOValueCoders(String arg0) {
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

    protected long getSID() {
        
        return (r.nextInt(Integer.MAX_VALUE - 1) + 1) << 2
                | ITermIdCodes.TERMID_CODE_STATEMENT;

    }

    /**
     * Return an array of {@link SPO}s.
     * 
     * @param n
     *            The #of elements in the array.
     * @param SIDs
     *            If statement identifiers should be generated.
     * @param inference
     *            if {@link StatementEnum}s should be assigned.
     * 
     * @return The array.
     */
    protected SPO[] getData(final int n, final boolean SIDs,
            final boolean inference) {

        final SPO[] a = new SPO[n];

        for (int i = 0; i < n; i++) {

            /*
             * Note: Only the {s,p,o} are assigned. The statement type and the
             * statement identifier are not part of the key for the statement
             * indices.
             */
            final SPO spo;
            
            if (SIDs && !inference) {
            
                // only explicit statements can have SIDs.
                spo = new SPO(getTermId(), getTermId(), getTermId(),
                        StatementEnum.Explicit);
                
                spo.setStatementIdentifier(getSID());
                
            } else if (inference) {
               
                final int tmp = r.nextInt(100);
                final StatementEnum type;
                if (tmp < 4) {
                    type = StatementEnum.Axiom;
                } else if (tmp < 60) {
                    type = StatementEnum.Explicit;
                } else {
                    type = StatementEnum.Inferred;
                }

                spo = new SPO(getTermId(), getTermId(), getTermId(), type);

                if (SIDs && type == StatementEnum.Explicit
                        && r.nextInt(100) < 20) {

                    // explicit statement with SID.
                    spo.setStatementIdentifier(getSID());

                }
                
            } else {

                // Explicit statement (no inference, no SIDs).
                spo = new SPO(getTermId(), getTermId(), getTermId(),
                        StatementEnum.Explicit);

            }
            
            a[i] = spo;
            
        }
        
        return a;
        
    }

    public void test_simpleCoder() {

        doRoundTripTests(SimpleRabaCoder.INSTANCE, false/* sids */, false/* inference */);
        doRoundTripTests(SimpleRabaCoder.INSTANCE, true/* sids */, false/* inference */);
        doRoundTripTests(SimpleRabaCoder.INSTANCE, false/* sids */, true/* inference */);
        doRoundTripTests(SimpleRabaCoder.INSTANCE, true/* sids */, true/* inference */);
        
    }

    public void test_FastRDFValueCoder() {

        doRoundTripTests(new FastRDFValueCompression(), false/* sids */, true/* inference */);

    }

    protected void doRoundTripTests(final IRabaCoder rabaCoder,
            final boolean SIDs, final boolean inference) {

        doRoundTripTest(getData(0, SIDs, inference), rabaCoder);

        doRoundTripTest(getData(1, SIDs, inference), rabaCoder);

        doRoundTripTest(getData(10, SIDs, inference), rabaCoder);

        doRoundTripTest(getData(100, SIDs, inference), rabaCoder);

        doRoundTripTest(getData(1000, SIDs, inference), rabaCoder);

        doRoundTripTest(getData(10000, SIDs, inference), rabaCoder);

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

        final SPOTupleSerializer tupleSer = new SPOTupleSerializer(
                SPOKeyOrder.SPO);
        
        final byte[][] vals = new byte[a.length][];
        {

            for (int i = 0; i < a.length; i++) {

                vals[i] = tupleSer.serializeVal(a[i]);

            }

        }
        final IRaba expected = new ReadOnlyValuesRaba(vals);

        /*
         * Compress the keys.
         */
        final AbstractFixedByteArrayBuffer originalData = rabaCoder.encode(
                expected, new DataOutputBuffer());

        try {

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

        } catch (Throwable t) {

            fail("Cause=" + t + ", expectedRaba=" + expected, t);

        }

    }

}
