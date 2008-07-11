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

package com.bigdata.rdf.lexicon;

import com.bigdata.btree.ASCIIKeyBuilderFactory;
import com.bigdata.btree.BytesUtil;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rdf.lexicon.Id2TermTupleSerializer;

import junit.framework.TestCase2;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestId2TermTupleSerializer extends TestCase2 {

    /**
     * 
     */
    public TestId2TermTupleSerializer() {
    }

    /**
     * @param arg0
     */
    public TestId2TermTupleSerializer(String arg0) {
        super(arg0);
    }

    public void test_id2key() {
        
        final Id2TermTupleSerializer fixture = new Id2TermTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_LONG));

        long id1 = -1;
        long id2 = 0;
        long id3 = 1;

        byte[] k1 = fixture.id2key(id1);
        byte[] k2 = fixture.id2key(id2);
        byte[] k3 = fixture.id2key(id3);

        System.err
                .println("k1(termId:" + id1 + ") = " + BytesUtil.toString(k1));
        System.err
                .println("k2(termId:" + id2 + ") = " + BytesUtil.toString(k2));
        System.err
                .println("k3(termId:" + id3 + ") = " + BytesUtil.toString(k3));

        /*
         * Verify that ids assigned in sequence result in an order for the
         * corresponding keys in the same sequence.
         */
        assertTrue(BytesUtil.compareBytes(k1, k2) < 0);
        assertTrue(BytesUtil.compareBytes(k2, k3) < 0);

    }

}
