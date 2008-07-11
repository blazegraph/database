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
import com.bigdata.rdf.rules.RDFSVocabulary;
import com.bigdata.rdf.rules.RuleOwlEquivalentProperty;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.Rule;

/**
 * Test suite for {@link RuleOwlEquivalentProperty}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleOwlEquivalentProperty extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleOwlEquivalentProperty() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleOwlEquivalentProperty(String name) {
        super(name);
    }

    /**
     * Test where the data satisifies the rule exactly once.
     * 
     * <pre>
     *  (a owl:equivalentProperty b) -&gt; (b owl:equivalentProperty a) 
     * </pre>
     * @throws Exception 
     */
    public void test_owlEquivalentProperty() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(A, OWL.EQUIVALENTPROPERTY, B);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(A, OWL.EQUIVALENTPROPERTY, B));
            assertEquals(1,store.getStatementCount());

            RDFSVocabulary inf = new RDFSVocabulary(store);
            
            Rule r = new RuleOwlEquivalentProperty(store.getSPORelation().getResourceIdentifier(),inf);

            // apply the rule.
            applyRule(store, r, -1/*solutionCount*/,1/*mutationCount*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(A, OWL.EQUIVALENTPROPERTY, B));

            // entailed
            assertTrue(store.hasStatement(B, OWL.EQUIVALENTPROPERTY, A));

            // final #of statements in the store.
            assertEquals(2,store.getStatementCount());

        } finally {

            store.closeAndDelete();

        }
        
    }
    
}
