/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Test suite for owl:SymmetricProperty processing.
 * 
 * <pre>
 * (x rdf:type owl:SymmetricProperty), (a x b) -&gt; (b x a)
 * </pre>
 * 
 * @see RuleOwlSymmetricProperty
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestRuleOwlSymmetricProperty extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleOwlSymmetricProperty() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleOwlSymmetricProperty(String name) {
        super(name);
    }

    public void test_Rule() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI X = new URIImpl("http://www.foo.org/X");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, RDF.TYPE, OWL.SYMMETRICPROPERTY);
            buffer.add(A, X, B);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, RDF.TYPE, OWL.SYMMETRICPROPERTY));
            assertTrue(store.hasStatement(A, X, B));
            final long nbefore = store.getStatementCount();

            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleOwlSymmetricProperty(store.getSPORelation()
                    .getNamespace(), vocab);

            // apply the rule.
            applyRule(store, r, -1/*solutionCount*/,1/*mutationCount*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(X, RDF.TYPE, OWL.SYMMETRICPROPERTY));
            assertTrue(store.hasStatement(A, X, B));

            // entailed
            assertTrue(store.hasStatement(B, X, A));

            // final #of statements in the store.
            assertEquals(nbefore + 1, store.getStatementCount());

        } finally {

            store.__tearDownUnitTest();

        }
        
    }

}
