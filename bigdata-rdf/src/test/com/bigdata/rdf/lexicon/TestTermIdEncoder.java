/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Mar 24, 2009
 */

package com.bigdata.rdf.lexicon;

import java.util.Random;

import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.TermIdEncoder.TermIdStat;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase2;

/**
 * Unit tests for {@link TermIdEncoder}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestTermIdEncoder extends TestCase2 {

    public TestTermIdEncoder() {
    }
    
    public TestTermIdEncoder(String name) {
        super(name);
    }

    /**
     * Correct rejection test for {@link IRawTripleStore#NULL}.
     */
    public void test_reject_NULL() {

        try {

            TermIdEncoder.encode(true/* scaleOut */, IRawTripleStore.NULL,
                    ITermIndexCodes.TERM_CODE_URI);
            
            fail("Expecting: " + IllegalArgumentException.class);
            
        } catch (IllegalArgumentException ex) {
            
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
            
        }

    }

    public void test_correctRejection_localCounter_zero() {

        try {

            TermIdEncoder.encodeScaleOut(0/* partitionId */, 0/* localCounter */,
                    ITermIndexCodes.TERM_CODE_URI);

            fail("Expecting: " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        }

    }

    public void test_correctRejection_localCounter_negative() {

        try {

            TermIdEncoder.encodeScaleOut(0/* partitionId */, -1/* localCounter */,
                    ITermIndexCodes.TERM_CODE_URI);

            fail("Expecting: " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        }

    }

    public void test_correctRejection_localCounter_tooLarge() {

        try {

            final int localCounter = Integer.MAX_VALUE / 2 + 1;
            
            TermIdEncoder.encodeScaleOut(0/* partitionId */, localCounter,
                    ITermIndexCodes.TERM_CODE_URI);

            fail("Expecting: " + IllegalArgumentException.class);

        } catch (IllegalArgumentException ex) {

            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);

        }

    }

    /**
     * Test with partition ZERO (0) and localCounter ONE (1), which are their
     * respective minimum legal values.
     */
    public void test_001() {

        // partitionId, localCounter, code
        doTermIdEncodeDecodeTest(0, 1, ITermIndexCodes.TERM_CODE_URI);
        doTermIdEncodeDecodeTest(0, 1, ITermIndexCodes.TERM_CODE_LIT);
        doTermIdEncodeDecodeTest(0, 1, ITermIndexCodes.TERM_CODE_BND);
        doTermIdEncodeDecodeTest(0, 1, ITermIndexCodes.TERM_CODE_STMT);

    }

    /**
     * Test with partitionId == {@link Integer#MAX_VALUE}
     */
    public void test_002() {

        // partitionId, localCounter, code
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, 1,
                ITermIndexCodes.TERM_CODE_URI);
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, 1,
                ITermIndexCodes.TERM_CODE_LIT);
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, 1,
                ITermIndexCodes.TERM_CODE_BND);
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, 1,
                ITermIndexCodes.TERM_CODE_STMT);

    }

    /**
     * Test with localCounter == {@link Integer#MAX_VALUE}/2, which is the
     * maximum allowed value.
     */
    public void test_003() {

        // partitionId, localCounter, code
        doTermIdEncodeDecodeTest(0, Integer.MAX_VALUE>>1,
                ITermIndexCodes.TERM_CODE_URI);
        doTermIdEncodeDecodeTest(0, Integer.MAX_VALUE>>1,
                ITermIndexCodes.TERM_CODE_LIT);
        doTermIdEncodeDecodeTest(0, Integer.MAX_VALUE>>1,
                ITermIndexCodes.TERM_CODE_BND);
        doTermIdEncodeDecodeTest(0, Integer.MAX_VALUE>>1,
                ITermIndexCodes.TERM_CODE_STMT);

    }

    /**
     * Test with the maximum allowed value for both the partitionId and the
     * localCounter.
     */
    public void test_004() {

        // partitionId, localCounter, code
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, Integer.MAX_VALUE >> 1,
                ITermIndexCodes.TERM_CODE_URI);
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, Integer.MAX_VALUE >> 1,
                ITermIndexCodes.TERM_CODE_LIT);
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, Integer.MAX_VALUE >> 1,
                ITermIndexCodes.TERM_CODE_BND);
        doTermIdEncodeDecodeTest(Integer.MAX_VALUE, Integer.MAX_VALUE >> 1,
                ITermIndexCodes.TERM_CODE_STMT);

    }
    
    /**
     * Stress test with 10M random trials.
     */
    public void test_stress() {

        final int ntrials = Bytes.megabyte32 * 10;

        final Random r = new Random();

        final byte[] codes = new byte[] {
                ITermIndexCodes.TERM_CODE_URI,
                ITermIndexCodes.TERM_CODE_LIT,
                ITermIndexCodes.TERM_CODE_BND,
                ITermIndexCodes.TERM_CODE_STMT,
        };        
        
        for (int i = 0; i < ntrials; i++) {

            // in [0:MaxInt-1], but any non-negative partitionId is allowed.
            final int partitionId = r.nextInt(Integer.MAX_VALUE);

            // in [0:maxLocalCounter]
            final int localCounter = r.nextInt(TermIdEncoder.maxLocalCounter) + 1;

            final byte termTypeCode = codes[r.nextInt(3)];

            doTermIdEncodeDecodeTest(partitionId, localCounter, termTypeCode);

        }

    }

    protected void doTermIdEncodeDecodeTest(final int partitionId,
            final int localCounter, final byte termTypeCode) {

        final long termId = TermIdEncoder.encodeScaleOut(partitionId,
                localCounter, termTypeCode);

        try {

            // must be non-zero because we don't generate NULL as a termId.
            assertNotSame(0L, termId);

            // must be non-negative because of how we pack the term ids.
            assertTrue(termId > 0L);

            // verify bit flags.
            switch (termTypeCode) {
            case ITermIndexCodes.TERM_CODE_URI:
                assertTrue(AbstractTripleStore.isURI(termId));
                break;
            case ITermIndexCodes.TERM_CODE_LIT:
                assertTrue(AbstractTripleStore.isLiteral(termId));
                break;
            case ITermIndexCodes.TERM_CODE_BND:
                assertTrue(AbstractTripleStore.isBNode(termId));
                break;
            case ITermIndexCodes.TERM_CODE_STMT:
                assertTrue(AbstractTripleStore.isStatement(termId));
                break;
            }

            // now decode the term identifier.
            final TermIdStat stat = TermIdEncoder.decodeScaleOut(termId);

            // verify the index partition identifier.
            assertEquals("partitionId", partitionId, stat.partitionId);

            // verify the local counter value.
            assertEquals("localCounter", localCounter, stat.localCounter);

            // verify the decoded bit flags.
            switch (termTypeCode) {
            case ITermIndexCodes.TERM_CODE_URI:
                assertTrue("isURI", stat.isURI());
                assertFalse("isLiteral", stat.isLiteral());
                assertFalse("isBNode", stat.isBNode());
                assertFalse("isSID", stat.isSID());
                break;
            case ITermIndexCodes.TERM_CODE_LIT:
            case ITermIndexCodes.TERM_CODE_DTL:
            case ITermIndexCodes.TERM_CODE_LCL:
                assertFalse("isURI", stat.isURI());
                assertTrue("isLiteral", stat.isLiteral());
                assertFalse("isBNode", stat.isBNode());
                assertFalse("isSID", stat.isSID());
                break;
            case ITermIndexCodes.TERM_CODE_BND:
                assertFalse("isURI", stat.isURI());
                assertFalse("isLiteral", stat.isLiteral());
                assertTrue("isBNode", stat.isBNode());
                assertFalse("isSID", stat.isSID());
                break;
            case ITermIndexCodes.TERM_CODE_STMT:
                assertFalse("isURI", stat.isURI());
                assertFalse("isLiteral", stat.isLiteral());
                assertFalse("isBNode", stat.isBNode());
                assertTrue("isSID", stat.isSID());
                break;
            }

        } catch (AssertionFailedError t) {

            System.err.println("\nERROR: " + t);
            System.err.println("termId=" + termId);
            System.err.println("termId=" + Long.toBinaryString(termId));
            t.printStackTrace(System.err);

            throw t;

        }

    }
    
}
