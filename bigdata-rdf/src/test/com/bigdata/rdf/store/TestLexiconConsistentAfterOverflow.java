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
 * Created on Apr 23, 2008
 */

package com.bigdata.rdf.store;

import com.bigdata.rdf.lexicon.Term2IdWriteProc;
import com.bigdata.rdf.model.OptimizedValueFactory._URI;

import junit.framework.TestCase;

/**
 * Test for consistent lexicon after overflow when using scale-out indices.
 * <p>
 * The term:id index is responsible for making a persistent assignment of term
 * identifiers for terms (including URIs, Literals, and Statement identifiers
 * but not blank nodes). This operation is performed by {@link Term2IdWriteProc}. It is
 * an UNISOLATED task and is therefore atomic and SHOULD always be consistent.
 * 
 * @deprecated unless I can develop a unit test for this problem.  Right now this
 * is just used to analyze the errors that are being reported.  I suspect that the
 * problem is actually with the commit protocol.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLexiconConsistentAfterOverflow extends
//        AbstractTripleStore
        TestCase {

    /**
     * 
     */
    public TestLexiconConsistentAfterOverflow() {
    }

    /**
     * @param name
     */
    public TestLexiconConsistentAfterOverflow(String name) {
        super(name);
    }
    
    /**
     * <pre>
     * Consistency problem: id=1514476U, val=[0, 1, 0, 93, 102, 105, 108, 101, 58, 47, 67, 58, 47, 68, 111, 99, 117, 109, 101, 110, 116, 115, 37, 50, 48, 97, 110, 100, 37, 50, 48, 83, 101, 116, 116, 105, 110, 103, 115, 47, 98, 116, 104, 111, 109, 112, 115, 111, 110, 47, 119, 111, 114, 107, 115, 112, 97, 99, 101, 47, 114, 100, 102, 45, 100, 97, 116, 97, 47, 108, 101, 104, 105, 103, 104, 47, 85, 49, 48, 47, 85, 110, 105, 118, 101, 114, 115, 105, 116, 121, 51, 95, 53, 46, 111, 119, 108], oldval=[0, 1, 0, 94, 102, 105, 108, 101, 58, 47, 67, 58, 47, 68, 111, 99, 117, 109, 101, 110, 116, 115, 37, 50, 48, 97, 110, 100, 37, 50, 48, 83, 101, 116, 116, 105, 110, 103, 115, 47, 98, 116, 104, 111, 109, 112, 115, 111, 110, 47, 119, 111, 114, 107, 115, 112, 97, 99, 101, 47, 114, 100, 102, 45, 100, 97, 116, 97, 47, 108, 101, 104, 105, 103, 104, 47, 85, 49, 48, 47, 85, 110, 105, 118, 101, 114, 115, 105, 116, 121, 51, 95, 49, 52, 46, 111, 119, 108]
     * </pre>
     */
    public void test_error() {
        
        final long id = 1514476L;

        final byte[] val = new byte[] {//
        0, 1, 0, 93, 102, 105, 108, 101, 58, 47, 67, 58, 47, 68, 111, 99, 117,
                109, 101, 110, 116, 115, 37, 50, 48, 97, 110, 100, 37, 50, 48,
                83, 101, 116, 116, 105, 110, 103, 115, 47, 98, 116, 104, 111,
                109, 112, 115, 111, 110, 47, 119, 111, 114, 107, 115, 112, 97,
                99, 101, 47, 114, 100, 102, 45, 100, 97, 116, 97, 47, 108, 101,
                104, 105, 103, 104, 47, 85, 49, 48, 47, 85, 110, 105, 118, 101,
                114, 115, 105, 116, 121, 51, 95, 53, 46, 111, 119, 108 };

        final byte[] old = new byte[] {//
        0, 1, 0, 94, 102, 105, 108, 101, 58, 47, 67, 58, 47, 68, 111, 99, 117,
                109, 101, 110, 116, 115, 37, 50, 48, 97, 110, 100, 37, 50, 48,
                83, 101, 116, 116, 105, 110, 103, 115, 47, 98, 116, 104, 111,
                109, 112, 115, 111, 110, 47, 119, 111, 114, 107, 115, 112, 97,
                99, 101, 47, 114, 100, 102, 45, 100, 97, 116, 97, 47, 108, 101,
                104, 105, 103, 104, 47, 85, 49, 48, 47, 85, 110, 105, 118, 101,
                114, 115, 105, 116, 121, 51, 95, 49, 52, 46, 111, 119, 108 };
        
        System.err.println("val.length="+val.length);
        System.err.println("old.length="+old.length);

        for(int i=0; i<val.length; i++) {
            
            if (val[i] != old[i]) {

                System.err.println("index=" + i + ", val=" + val[i]
                        + ", but old=" + old[i]);
                
            }
            
        }

        System.err.println("isURI: "+AbstractTripleStore.isURI(id));
        System.err.println("isLIT: "+AbstractTripleStore.isLiteral(id));
        System.err.println("isBND: "+AbstractTripleStore.isBNode(id));
        System.err.println("isSID: "+AbstractTripleStore.isStatement(id));

//        WormAddressManager am = new WormAddressManager(32);
//
//        System.err.println("low word : "+am.getByteCount(id));
//        System.err.println("high word : "+am.getOffset(id));

        System.err.println("low word : "+(id&0xffffffffL));
        System.err.println("high word : "+(id&0xffffffffL<<32));

        System.err.println(_URI.deserialize(val));
        System.err.println(_URI.deserialize(old));
        
    }

}
