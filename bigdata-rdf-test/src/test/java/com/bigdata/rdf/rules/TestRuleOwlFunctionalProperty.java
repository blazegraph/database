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
import com.bigdata.util.InnerCause;
import com.bigdata.util.concurrent.ExecutionExceptions;

/**
 * Test suite for owl:FunctionalProperty processing.
 * 
 * <pre>
 * (x rdf:type owl:FunctionalProperty), (a x b), (a x c) -&gt; 
 * throw ConstraintViolationException
 * </pre>
 * 
 * @see RuleOwlFunctionalProperty
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 */
public class TestRuleOwlFunctionalProperty extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleOwlFunctionalProperty() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleOwlFunctionalProperty(String name) {
        super(name);
    }

    public void test_RuleOwlFunctionalProperty() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI john = new URIImpl("http://www.foo.org/john");
            URI mary = new URIImpl("http://www.foo.org/mary");
            URI susie = new URIImpl("http://www.foo.org/susie");
            URI wife = new URIImpl("http://www.foo.org/wife");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(wife, RDF.TYPE, OWL.FUNCTIONALPROPERTY);
            buffer.add(john, wife, mary);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(wife, RDF.TYPE, OWL.FUNCTIONALPROPERTY));
            assertTrue(store.hasStatement(john, wife, mary));
            final long nbefore = store.getStatementCount();

            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleOwlFunctionalProperty(store.getSPORelation()
                    .getNamespace(), vocab);

            // apply the rule.
            applyRule(store, r, -1/*solutionCount*/,0/*mutationCount*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(wife, RDF.TYPE, OWL.FUNCTIONALPROPERTY));
            assertTrue(store.hasStatement(john, wife, mary));

            // final #of statements in the store.
            assertEquals(nbefore, store.getStatementCount());

            // add another value for the functional property
            buffer.add(john, wife, susie);
            buffer.flush();

            applyRule(store, r, -1/*solutionCount*/, 2/*mutationCount*/);
            
            assertTrue(store.hasStatement(mary, OWL.SAMEAS, susie));
            assertTrue(store.hasStatement(susie, OWL.SAMEAS, mary));

            // final #of statements in the store.
            assertEquals(nbefore+3, store.getStatementCount());
            
            
//            try {
//            	
//	            // apply the rule.
//	            applyRule(store, r, -1/*solutionCount*/,0/*mutationCount*/);
//	            
//	            fail("should have violated the functional property");
//	            
//            } catch (Exception ex) {
//            	
//                if (!InnerCause.isInnerCause(ex,
//                        ConstraintViolationException.class)) {
//                    fail("Expected: " + ConstraintViolationException.class);
//                }
//                
////            	final ExecutionExceptions ex2 = (ExecutionExceptions)
////            			InnerCause.getInnerCause(ex, ExecutionExceptions.class);
////            	
////            	Throwable t = ex2.causes().get(0);
////            	while (t.getCause() != null)
////            		t = t.getCause();
////            	
////            	if (!(t instanceof ConstraintViolationException))
////            		fail("inner cause should be a ConstraintViolationException");
//            	
//            }

        } finally {

            store.__tearDownUnitTest();

        }
        
    }

    public void test_RuleOwlInverseFunctionalProperty() throws Exception {

        AbstractTripleStore store = getStore();

        try {

            URI john = new URIImpl("http://www.foo.org/john");
            URI paul = new URIImpl("http://www.foo.org/paul");
            URI mary = new URIImpl("http://www.foo.org/mary");
            URI wife = new URIImpl("http://www.foo.org/wife");

            IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);
            
            buffer.add(wife, RDF.TYPE, OWL.INVERSEFUNCTIONALPROPERTY);
            buffer.add(john, wife, mary);

            // write on the store.
            buffer.flush();

            // verify statement(s).
            assertTrue(store.hasStatement(wife, RDF.TYPE, OWL.INVERSEFUNCTIONALPROPERTY));
            assertTrue(store.hasStatement(john, wife, mary));
            final long nbefore = store.getStatementCount();

            final Vocabulary vocab = store.getVocabulary();

            final Rule r = new RuleOwlInverseFunctionalProperty(store.getSPORelation()
                    .getNamespace(), vocab);

            // apply the rule.
            applyRule(store, r, -1/*solutionCount*/,0/*mutationCount*/);

            /*
             * validate the state of the primary store.
             */

            // told
            assertTrue(store.hasStatement(wife, RDF.TYPE, OWL.INVERSEFUNCTIONALPROPERTY));
            assertTrue(store.hasStatement(john, wife, mary));

            // final #of statements in the store.
            assertEquals(nbefore, store.getStatementCount());

            // add another S for the inverse functional property
            buffer.add(paul, wife, mary);
            buffer.flush();

            applyRule(store, r, -1/*solutionCount*/, 2/*mutationCount*/);
            
            assertTrue(store.hasStatement(john, OWL.SAMEAS, paul));
            assertTrue(store.hasStatement(paul, OWL.SAMEAS, john));

            // final #of statements in the store.
            assertEquals(nbefore+3, store.getStatementCount());
            
//            try {
//            	
//	            // apply the rule.
//	            applyRule(store, r, -1/*solutionCount*/,0/*mutationCount*/);
//	            
//	            fail("should have violated the inverse functional property");
//	            
//            } catch (Exception ex) {
//            	
//                if (!InnerCause.isInnerCause(ex,
//                        ConstraintViolationException.class)) {
//                    fail("Expected: " + ConstraintViolationException.class);
//                }
////            	final ExecutionExceptions ex2 = (ExecutionExceptions)
////            			InnerCause.getInnerCause(ex, ExecutionExceptions.class);
////            	
////            	Throwable t = ex2.causes().get(0);
////            	while (t.getCause() != null)
////            		t = t.getCause();
////            	
////            	if (!(t instanceof ConstraintViolationException))
////            		fail("inner cause should be a ConstraintViolationException");
//            	
//            }

        } finally {

            store.__tearDownUnitTest();

        }
        
    }

}
