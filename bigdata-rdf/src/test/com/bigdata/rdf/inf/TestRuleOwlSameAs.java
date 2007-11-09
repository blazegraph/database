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

package com.bigdata.rdf.inf;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.vocabulary.OWL;

import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for owl:sameAs processing.
 * 
 * <pre>
 *   owl:sameAs1: (x owl:sameAs y) -&gt; (y owl:sameAs x)
 *   owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
 *   owl:sameAs3: (x owl:sameAs y), (z a x) -&gt; (z a y).
 * </pre>
 * 
 * @see RuleOwlSameAs1
 * @see RuleOwlSameAs2
 * @see RuleOwlSameAs3
 * 
 * @todo Review the use of constraints on these rules. Are we able to break
 *       cycles using constaints? If so, then the rules do not need to be
 *       brought to fix point as a set but can instead by closed individually.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleOwlSameAs extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleOwlSameAs() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleOwlSameAs(String name) {
        super(name);
    }

    /**
     * Test where the data satisifies the rule exactly once.
     * 
     * <pre>
     * owl:sameAs1: (x owl:sameAs y) -&gt; (y owl:sameAs x)
     * </pre>
     */
    public void test_owlSameAs1() {

        AbstractTripleStore store = getStore();

        try {

            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, new URIImpl(OWL.SAMEAS), Y);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertEquals(1,store.getStatementCount());

            InferenceEngine inf = new InferenceEngine(store);

            // apply the rule.
            RuleStats stats = applyRule(inf,inf.ruleOwlSameAs1, 1/*expectedComputed*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));

            // entailed
            assertTrue(store.hasStatement(Y, new URIImpl(OWL.SAMEAS), X));

            // final #of statements in the store.
            assertEquals(2,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }
        
    }

    /**
     * Test where the data satisifies the rule exactly once.
     * <p>
     * Note: This also verifies that we correctly filter out entailments where
     * <code>a == owl:sameAs</code>.
     * 
     * <pre>
     *  owl:sameAs2: (x owl:sameAs y), (x a z) -&gt; (y a z).
     * </pre>
     */
    public void test_owlSameAs2() {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI Z = new URIImpl("http://www.foo.org/Z");
            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, new URIImpl(OWL.SAMEAS), Y);
            buffer.add(X, A, Z);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(X, A, Z));
            assertEquals(2,store.getStatementCount());

            InferenceEngine inf = new InferenceEngine(store);

            // apply the rule.
            RuleStats stats = applyRule(inf,inf.ruleOwlSameAs2, 1/*expectedComputed*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(X, A, Z));

            // entailed
            assertTrue(store.hasStatement(Y, A, Z));

            // final #of statements in the store.
            assertEquals(3,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }
        
    }
    
    /**
     * Test where the data satisifies the rule exactly once.
     * <p>
     * Note: This also verifies that we correctly filter out entailments where
     * <code>a == owl:sameAs</code>.
     * 
     * <pre>
     * owl:sameAs3: (x owl:sameAs y), (z a x) -&gt; (z a y).
     * </pre>
     */
    public void test_owlSameAs3() {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI Z = new URIImpl("http://www.foo.org/Z");
            URI X = new URIImpl("http://www.foo.org/X");
            URI Y = new URIImpl("http://www.foo.org/Y");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(X, new URIImpl(OWL.SAMEAS), Y);
            buffer.add(Z, A, X);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(Z, A, X));
            assertEquals(2,store.getStatementCount());

            InferenceEngine inf = new InferenceEngine(store);

            // apply the rule.
            RuleStats stats = applyRule(inf,inf.ruleOwlSameAs3, 1/*expectedComputed*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(X, new URIImpl(OWL.SAMEAS), Y));
            assertTrue(store.hasStatement(Z, A, X));

            // entailed
            assertTrue(store.hasStatement(Z, A, Y));

            // final #of statements in the store.
            assertEquals(3,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }
        
    }
    
}
