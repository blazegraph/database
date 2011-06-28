/**

Copyright (C) SYSTAP, LLC 2006-2009.  All rights reserved.

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
 * Test suite for owl:hasValue processing.
 * 
 * <pre>
 * (x rdf:type a), (a rdf:type owl:Restriction), (a owl:onProperty p), (a owl:hasValue v) -&gt; (x p v)
 * </pre>
 * 
 * @see RuleOwlHasValue
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestRuleOwlHasValue extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleOwlHasValue() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleOwlHasValue(String name) {
        super(name);
    }

    /**
     * Test where the data satisifies the rule exactly once.
     * 
     * <pre>
     * (x rdf:type a), (a rdf:type owl:Restriction), (a owl:onProperty p), (a owl:hasValue v) -&gt; (x p v)
     * </pre>
     * @throws Exception 
     */
    public void test_OwlHasValue() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI X = new URIImpl("http://www.foo.org/X");
            URI P = new URIImpl("http://www.foo.org/P");
            URI V = new URIImpl("http://www.foo.org/V");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(A, RDF.TYPE, X);
            buffer.add(X, RDF.TYPE, OWL.RESTRICTION);
            buffer.add(X, OWL.ONPROPERTY, P);
            buffer.add(X, OWL.HASVALUE, V);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(A, RDF.TYPE, X));
            assertTrue(store.hasStatement(X, RDF.TYPE, OWL.RESTRICTION));
            assertTrue(store.hasStatement(X, OWL.ONPROPERTY, P));
            assertTrue(store.hasStatement(X, OWL.HASVALUE, V));
            final long nbefore = store.getStatementCount();

            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleOwlHasValue(store.getSPORelation()
                    .getNamespace(), vocab);

            // apply the rule.
            applyRule(store, r, -1/*solutionCount*/,1/*mutationCount*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(A, RDF.TYPE, X));
            assertTrue(store.hasStatement(X, RDF.TYPE, OWL.RESTRICTION));
            assertTrue(store.hasStatement(X, OWL.ONPROPERTY, P));
            assertTrue(store.hasStatement(X, OWL.HASVALUE, V));

            // entailed
            assertTrue(store.hasStatement(A, P, V));

            // final #of statements in the store.
            assertEquals(nbefore + 1, store.getStatementCount());

        } finally {

            store.__tearDownUnitTest();

        }
        
    }

}
