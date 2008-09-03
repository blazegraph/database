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
 * Created on Nov 2, 2007
 */

package com.bigdata.rdf.rules;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.Rule;

/**
 * Test suite for owl:transtitiveProperty processing.
 * 
 * <pre>
 * (a rdf:type owl:TransitiveProperty), (x a y), (y a z) -&gt; (x a z)
 * </pre>
 * 
 * @see RuleOwlTranstitiveProperty
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleOwlTransitiveProperty extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleOwlTransitiveProperty() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleOwlTransitiveProperty(String name) {
        super(name);
    }

    /**
     * Test where the data satisifies the rule exactly once.
     * 
     * <pre>
     * (a rdf:type owl:TransitiveProperty), (x a y), (y a z) -&gt; (x a z)
     * </pre>
     * @throws Exception 
     */
    public void test_OwlTranstitiveProperty1() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");
            URI Z = new URIImpl("http://www.foo.org/Z");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(A, RDF.TYPE, OWL.TRANSITIVEPROPERTY);
            buffer.add(X, A, Y);
            buffer.add(Y, A, Z);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(A, RDF.TYPE, OWL.TRANSITIVEPROPERTY));
            assertTrue(store.hasStatement(X, A, Y));
            assertTrue(store.hasStatement(Y, A, Z));
            final long nbefore = store.getStatementCount();

            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleOwlTransitiveProperty(store.getSPORelation()
                    .getNamespace(), vocab);

            // apply the rule.
            applyRule(store, r, -1/*solutionCount*/,1/*mutationCount*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(A, RDF.TYPE, OWL.TRANSITIVEPROPERTY));
            assertTrue(store.hasStatement(X, A, Y));
            assertTrue(store.hasStatement(Y, A, Z));

            // entailed
            assertTrue(store.hasStatement(X, A, Z));

            // final #of statements in the store.
            assertEquals(nbefore + 1, store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }
        
    }

}
