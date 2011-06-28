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
 * Created on Apr 18, 2007
 */

package com.bigdata.rdf.rules;

import java.util.Properties;

import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.rule.Rule;

/**
 * Test suite for {@link RuleRdf01}.
 * 
 * <pre>
 *   triple(?v rdf:type rdf:Property) :-
 *      triple( ?u ?v ?x ).
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleRdf01 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleRdf01() {
    }

    /**
     * @param name
     */
    public TestRuleRdf01(String name) {
        super(name);
    }

    /**
     * Basic test of rule semantics.
     */
    public void test_rdf01() throws Exception {
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);

        try {
            
            final BigdataValueFactory f = store.getValueFactory();
            
            final URI A = f.createURI("http://www.foo.org/A");
            final URI B = f.createURI("http://www.foo.org/B");
            final URI C = f.createURI("http://www.foo.org/C");
    
            final URI rdfType = RDF.TYPE;
            final URI rdfProperty = RDF.PROPERTY;
    
            store.addStatement(A, B, C);
    
            assertTrue(store.hasStatement(A, B, C));
            assertFalse(store.hasStatement(B, rdfType, rdfProperty ));
            assertEquals(1,store.getStatementCount());
    
            final Rule r = new RuleRdf01(store.getSPORelation().getNamespace(),
                    store.getVocabulary());
            
            applyRule(store, r, 1/* solutionCount*/,1/*mutationCount*/);
            
            /*
             * validate the state of the primary store.
             */
            assertTrue(store.hasStatement(A, B, C));
            assertTrue(store.hasStatement(B, rdfType, rdfProperty ));
            assertEquals(2,store.getStatementCount());
        
        } finally {
        
            store.__tearDownUnitTest();
            
        }
        
    }

    /**
     * Test that can be used to verify that we are doing an efficient scan for
     * the distinct predicates (distinct key prefix scan).
     */
    public void test_rdf01_distinctPrefixScan() throws Exception {
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);

        try {

            final BigdataValueFactory f = store.getValueFactory();
            
            final URI A = f.createURI("http://www.foo.org/A");
            final URI B = f.createURI("http://www.foo.org/B");
            final URI C = f.createURI("http://www.foo.org/C");
            final URI D = f.createURI("http://www.foo.org/D");
            final URI E = f.createURI("http://www.foo.org/E");

            final URI rdfType = RDF.TYPE;
            final URI rdfProperty = RDF.PROPERTY;

            /*
             * Three statements that will trigger the rule, but two statements
             * share the same predicate. When it does the minimum amount of
             * work, the rule will fire for each distinct predicate in the KB --
             * for this KB that is only twice.
             */
            store.addStatement(A, B, C);
            store.addStatement(C, B, D);
            store.addStatement(A, E, C);

            assertTrue(store.hasStatement(A, B, C));
            assertTrue(store.hasStatement(C, B, D));
            assertTrue(store.hasStatement(A, E, C));
            assertFalse(store.hasStatement(B, rdfType, rdfProperty));
            assertFalse(store.hasStatement(E, rdfType, rdfProperty));
            assertEquals(3,store.getStatementCount());

            final Rule r = new RuleRdf01(store.getSPORelation().getNamespace(),
                    store.getVocabulary());

            applyRule(store, r, 2/* solutionCount */, 2/* mutationCount */);

            /*
             * validate the state of the primary store.
             */
            assertTrue(store.hasStatement(A, B, C));
            assertTrue(store.hasStatement(C, B, D));
            assertTrue(store.hasStatement(A, E, C));
            assertTrue(store.hasStatement(B, rdfType, rdfProperty));
            assertTrue(store.hasStatement(E, rdfType, rdfProperty));
            assertEquals(5,store.getStatementCount());

        } finally {

            store.__tearDownUnitTest();

        }
        
    }
    
}
