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
 * Created on Jul 7, 2008
 */

package com.bigdata.rdf.rules;

import java.util.Arrays;
import java.util.Properties;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine.Options;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDistinctTermScan extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestDistinctTermScan() {
    }

    /**
     * @param name
     */
    public TestDistinctTermScan(String name) {
        super(name);
    }

    /**
     * FIXME The distinct term scan has been moved into
     * {@link AbstractRuleDistinctTermScan} and needs to be evaluated in that
     * context.
     */
    public void test_getDistinctTermIdentifiers() {

        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);

        try {


            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");
            URI D = new URIImpl("http://www.foo.org/D");
            URI E = new URIImpl("http://www.foo.org/E");

            {
                IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);

                buffer.add(A, B, C);
                buffer.add(C, B, D);
                buffer.add(A, E, C);

                // flush statements to the store.
                buffer.flush();
            }
            
            assertTrue(store.hasStatement(A, B, C));
            assertTrue(store.hasStatement(C, B, D));
            assertTrue(store.hasStatement(A, E, C));

            // distinct subject term identifiers.
            {

                Long[] expected = new Long[] {

                store.getTermId(A), store.getTermId(C)

                };

                // term identifers will be in ascending order.
                Arrays.sort(expected);

                assertSameItr(expected, store.getSPORelation()
                        .distinctTermScan(SPOKeyOrder.SPO));

            }

            // distinct predicate term identifiers.
            {

                Long[] expected = new Long[] {

                store.getTermId(B), store.getTermId(E)

                };

                // term identifers will be in ascending order.
                Arrays.sort(expected);

                assertSameItr(expected, store.getSPORelation()
                        .distinctTermScan(SPOKeyOrder.POS));

            }

            // distinct object term identifiers.
            {

                Long[] expected = new Long[] {

                store.getTermId(C), store.getTermId(D)

                };

                // term identifers will be in ascending order.
                Arrays.sort(expected);

                assertSameItr(expected, store.getSPORelation()
                        .distinctTermScan(SPOKeyOrder.OSP));

            }

        } finally {

            store.closeAndDelete();

        }

    }

}
