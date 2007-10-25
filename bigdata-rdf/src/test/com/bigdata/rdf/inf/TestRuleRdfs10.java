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
 * Created on Apr 13, 2007
 */

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Note: rdfs 6, 8, 10, 12, and 13 use the same base clase.
 * 
 * @see RuleRdfs06
 * @see RuleRdfs08
 * @see RuleRdfs10
 * @see RuleRdfs12
 * @see RuleRdfs13
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdfs10 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdfs10() {
    }

    /**
     * @param name
     */
    public TestRuleRdfs10(String name) {
        super(name);
    }

    /**
     * Test of {@link RuleRdfs10} where the data satisifies the rule exactly
     * once.
     * 
     * <pre>
     *         triple(?u,rdfs:subClassOf,?u) :-
     *            triple(?u,rdf:type,rdfs:Class). 
     * </pre>
     */
    public void test_rdfs10_01() {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");

            StatementBuffer buffer = new StatementBuffer(store,
                    100/* capacity */, true/* distinct */);
            
            buffer.add(A, URIImpl.RDF_TYPE, URIImpl.RDFS_CLASS);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.containsStatement(A, URIImpl.RDF_TYPE, URIImpl.RDFS_CLASS));

            InferenceEngine inf = new InferenceEngine(store);

            // apply the rule.
            RuleStats stats = applyRule(inf.rdfs10, 1/*expectedComputed*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.containsStatement(A, URIImpl.RDF_TYPE, URIImpl.RDFS_CLASS));
            
            // entailed
            assertTrue(store.containsStatement(A, URIImpl.RDFS_SUBCLASSOF, A));

        } finally {

            store.closeAndDelete();

        }
        
    }
        
}
