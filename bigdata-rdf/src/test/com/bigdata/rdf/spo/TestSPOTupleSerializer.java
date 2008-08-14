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
 * Created on Jul 8, 2008
 */

package com.bigdata.rdf.spo;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractTuple;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.rdf.lexicon.ITermIdCodes;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOTupleSerializer;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestSPOTupleSerializer extends TestCase2 {

    /**
     * 
     */
    public TestSPOTupleSerializer() {
    }

    /**
     * @param arg0
     */
    public TestSPOTupleSerializer(String arg0) {
        super(arg0);
    }

    public void test_statementOrder() {

        SPOTupleSerializer fixture = new SPOTupleSerializer(SPOKeyOrder.SPO);
        
        byte[] k1 = fixture.statement2Key(1, 2, 3);
        byte[] k2 = fixture.statement2Key(2, 2, 3);
        byte[] k3 = fixture.statement2Key(2, 2, 4);
        
        System.err.println("k1(1,2,2) = "+BytesUtil.toString(k1));
        System.err.println("k2(2,2,3) = "+BytesUtil.toString(k2));
        System.err.println("k3(2,2,4) = "+BytesUtil.toString(k3));
        
        assertTrue(BytesUtil.compareBytes(k1, k2)<0);
        assertTrue(BytesUtil.compareBytes(k2, k3)<0);

    }
    
    public void test_encodeDecode() {

        doEncodeDecodeTest(new SPO(1, 2, 3, StatementEnum.Axiom),
                SPOKeyOrder.SPO);

        doEncodeDecodeTest(new SPO(1, 2, 3, StatementEnum.Explicit),
                SPOKeyOrder.POS);

        doEncodeDecodeTest(new SPO(1, 2, 3, StatementEnum.Inferred),
                SPOKeyOrder.OSP);

        /*
         * Note: [sid] is a legal statement identifier.
         * 
         * Note: Only explicit statements may have statement identifiers.
         */
        final long sid = 1 << 2 | ITermIdCodes.TERMID_CODE_STATEMENT;

        {

            final SPO spo = new SPO(3, 1, 2, StatementEnum.Explicit);

            spo.setStatementIdentifier(sid);

            doEncodeDecodeTest(spo, SPOKeyOrder.SPO);

        }

        {

            final SPO spo = new SPO(3, 1, 2, StatementEnum.Explicit);

            spo.setStatementIdentifier(sid);

            doEncodeDecodeTest(spo, SPOKeyOrder.POS);

        }

        {

            final SPO spo = new SPO(3, 1, 2, StatementEnum.Explicit);

            spo.setStatementIdentifier(sid);
            
            doEncodeDecodeTest(spo, SPOKeyOrder.OSP);
            
        }
        

    }
    
    protected void doEncodeDecodeTest(final SPO expected, SPOKeyOrder keyOrder) {
        
        final SPOTupleSerializer fixture = new SPOTupleSerializer(
                keyOrder);

        // encode key
        byte[] key = fixture.serializeKey(expected);

        // encode value.
        byte[] val = fixture.serializeVal(expected);

        /*
         * verify decoding.
         */
        final TestTuple<SPO> tuple = new TestTuple<SPO>(IRangeQuery.KEYS
                | IRangeQuery.VALS) {

            public ITupleSerializer getTupleSerializer() {
                return fixture;
            }

        };

        // copy data into the test tuple.
        tuple.copyTuple(key, val);

        final SPO actual = tuple.getObject();

        if (!expected.equals(actual)) {
            
            fail("Expected: "+expected+", but actual="+actual);
            
        }
        
        if (expected.hasStatementType()) {

            assertEquals("type", expected.getStatementType(), actual.getStatementType());

        }
        
        if(expected.hasStatementIdentifier()) {
            
            assertEquals("statementIdentifier", expected
                    .getStatementIdentifier(), actual.getStatementIdentifier());

        }

    }

    abstract private static class TestTuple<E> extends AbstractTuple<E> {

        public TestTuple(int flags) {
            
            super(flags);
            
        }

        public int getSourceIndex() {
            throw new UnsupportedOperationException();
        }

        // exposed for unit test.
        public void copyTuple(byte[] key, byte[] val) {
            
            super.copyTuple(key, val);
            
        }
    };
    
}
