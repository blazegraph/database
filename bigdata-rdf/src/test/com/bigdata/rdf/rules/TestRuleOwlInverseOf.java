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

import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.rule.Rule;

/**
 * Test suite for owl:inverseOf processing.
 * 
 * <pre>
 *   owl:inverseOf1 : (a owl:inverseOf b) -&gt; (b owl:inverseOf a)
 *   owl:inverseOf2 : (a owl:inverseOf b), (x a z) -&gt; (z b x).
 * </pre>
 * 
 * @see RuleOwlInverseOf1
 * @see RuleOwlInverseOf2
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleOwlInverseOf extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleOwlInverseOf() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleOwlInverseOf(String name) {
        super(name);
    }

    /**
     * Test where the data satisifies the rule exactly once.
     * 
     * <pre>
     *   owl:InverseOf1 : (a owl:inverseOf b) -&gt; (b owl:inverseOf a)
     * </pre>
     * @throws Exception 
     */
    public void test_owlInverseOf1() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, OWL.INVERSEOF, Y);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, OWL.INVERSEOF, Y));
            final long nbefore = store.getStatementCount();

            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleOwlInverseOf1(store.getSPORelation()
                    .getNamespace(), vocab);

            // apply the rule.
            applyRule(store, r, -1/*solutionCount*/,1/*mutationCount*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(X, OWL.INVERSEOF, Y));

            // entailed
            assertTrue(store.hasStatement(Y, OWL.INVERSEOF, X));

            // final #of statements in the store.
            assertEquals(nbefore + 1, store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }
        
    }

    /**
     * Test where the data satisifies the rule exactly once.
     * 
     * <pre>
     *   owl:InverseOf2 : (a owl:inverseOf b), (x a z) -&gt; (z b x).
     * </pre>
     * @throws Exception 
     */
    public void test_owlInverseOf2() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI Z = new URIImpl("http://www.foo.org/Z");
            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(A, OWL.INVERSEOF, B);
            buffer.add(X, A, Z);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(A, OWL.INVERSEOF, B));
            assertTrue(store.hasStatement(X, A, Z));
            final long nbefore = store.getStatementCount();

            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleOwlInverseOf2(store.getSPORelation()
                    .getNamespace(), vocab);

            // apply the rule.
            applyRule(store, r, -1/* solutionCount */, 1/* mutationCount */);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(A, OWL.INVERSEOF, B));
            assertTrue(store.hasStatement(X, A, Z));

            // entailed
            assertTrue(store.hasStatement(Z, B, X));

            // final #of statements in the store.
            assertEquals(nbefore + 1, store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }
        
    }
    
}
