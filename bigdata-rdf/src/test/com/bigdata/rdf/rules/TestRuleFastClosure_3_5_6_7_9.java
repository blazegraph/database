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
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.rules;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.bop.IConstant;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.IStatementBuffer;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.AbstractRuleFastClosure_3_5_6_7_9.FastClosureRuleTask;
import com.bigdata.rdf.rules.AbstractRuleFastClosure_3_5_6_7_9.SubPropertyClosureTask;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.Vocabulary;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IRuleTaskFactory;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;

/**
 * Test suite for {@link AbstractRuleFastClosure_3_5_6_7_9}.
 * 
 * @see RuleFastClosure3
 * @see RuleFastClosure5
 * @see RuleFastClosure6
 * @see RuleFastClosure7
 * @see RuleFastClosure9
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleFastClosure_3_5_6_7_9 extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleFastClosure_3_5_6_7_9() {
        super();
    }

    /**
     * @param name
     */
    public TestRuleFastClosure_3_5_6_7_9(String name) {
        super(name);
    }

    /**
     * Unit test for
     * {@link InferenceEngine#getSubProperties(AbstractTripleStore)}, which is
     * used to setup the pre-conditions for {@link RuleFastClosure3}.
     */
    public void test_getSubProperties() {

        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);
        
        try {

            final BigdataValueFactory valueFactory = store.getValueFactory();
            
            final URI A = valueFactory.createURI("http://www.foo.org/A");
            final URI B = valueFactory.createURI("http://www.foo.org/B");
            final URI C = valueFactory.createURI("http://www.foo.org/C");

            final URI rdfsSubPropertyOf = valueFactory.asValue(RDFS.SUBPROPERTYOF);

            store.addStatement(A, rdfsSubPropertyOf, rdfsSubPropertyOf);
            store.addStatement(B, rdfsSubPropertyOf, A);

            assertTrue(store.hasStatement(A, rdfsSubPropertyOf,
                    rdfsSubPropertyOf));
            assertTrue(store.hasStatement(B, rdfsSubPropertyOf, A));

            final Vocabulary vocab = store.getVocabulary();

            final IRelation<ISPO> view = store.getSPORelation();
            
            final SubPropertyClosureTask task = new SubPropertyClosureTask(
                    view, vocab.getConstant(RDFS.SUBPROPERTYOF));

            Set<IV> subProperties = task.getSubProperties();

            assertTrue(subProperties.contains(store
                    .getIV(rdfsSubPropertyOf)));
            assertTrue(subProperties.contains(store.getIV(A)));
            assertTrue(subProperties.contains(store.getIV(B)));

            assertEquals(3, subProperties.size());

            store.addStatement(C, A, A);

            assertTrue(store.hasStatement(C, A, A));

            subProperties = task.getSubProperties();

            assertTrue(subProperties.contains(store
                    .getIV(rdfsSubPropertyOf)));
            assertTrue(subProperties.contains(store.getIV(A)));
            assertTrue(subProperties.contains(store.getIV(B)));
            assertTrue(subProperties.contains(store.getIV(C)));

            assertEquals(4, subProperties.size());

        } finally {

            store.__tearDownUnitTest();

        }
        
    }

    /**
     * Unit test of {@link RuleFastClosure6} where the data allow the rule to
     * fire exactly twice, once where the predicate is <code>rdfs:Range</code>
     * and once where the predicate is an
     * <code>rdfs:subPropertyOf</code> <code>rdfs:Range</code>, and tests
     * that the rule correctly filters out a possible entailment that would
     * simply conclude its own support.
     * 
     * <pre>
     *      (?x, P, ?y) -&gt; (?x, propertyId, ?y)
     * </pre>
     * 
     * where <i>propertyId</i> is rdfs:Range
     * 
     * @throws Exception
     */
    public void test_rule() throws Exception {
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);
        
        try {

            URI A = new URIImpl("http://www.foo.org/A");
            URI B = new URIImpl("http://www.foo.org/B");
            URI C = new URIImpl("http://www.foo.org/C");
            URI D = new URIImpl("http://www.foo.org/D");

            URI MyRange = new URIImpl("http://www.foo.org/MyRange");

            {
                IStatementBuffer buffer = new StatementBuffer(store, 100/* capacity */);

                buffer.add(A, RDFS.RANGE, B);

                buffer.add(C, MyRange, D);

                // write on the store.
                buffer.flush();

                // verify the database.

                assertTrue(store.hasStatement(A, RDFS.RANGE, B));

                assertTrue(store.hasStatement(C, MyRange, D));

                assertEquals(2, store.getStatementCount());
            }
            
            /*
             * Setup the closure of the rdfs:subPropertyOf for rdfs:Range.
             * 
             * Note: This includes both rdfs:Range and the MyRange URI. The
             * latter is not declared by the ontology to be a subPropertyOf
             * rdfs:Range since that is not required by this test, but we are
             * treating it as such for the purpose of this test.
             */

            Set<IV> R = new HashSet<IV>();

            R.add(store.getIV(RDFS.RANGE));

            R.add(store.getIV(MyRange));

            /*
             * setup the rule execution.
             */

            final Vocabulary vocab = store.getVocabulary();

            Rule rule = new MyRuleFastClosure6("myFastClosure6", store
                    .getSPORelation().getNamespace(), null/* focusStore */,
                    vocab.getConstant(RDFS.SUBPROPERTYOF), //
                    vocab.getConstant(RDFS.RANGE), R);
            
            applyRule(store, rule, -1/*@todo 2? solutionCount*/,1/*mutationCount*/);

            // told.
            
            assertTrue(store.hasStatement(A, RDFS.RANGE, B));

            assertTrue(store.hasStatement(C, MyRange, D));

            /*
             * entailed
             * 
             * Note: The 2nd possible entailment is (A rdfs:Range B), which is
             * already an explicit statement in the database. The rule refuses
             * to consider triple patterns where the predicate is the same as
             * the predicate on the entailment since the support would then
             * entail itself.
             */

            assertTrue(store.hasStatement(C, RDFS.RANGE, D));
            
        } finally {
            
            store.__tearDownUnitTest();
            
        }
        
    }
    
    /**
     * Implementation with the semantics of {@link RuleFastClosure6} but
     * accepting a hand-built property set closure.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    @SuppressWarnings("serial")
    private static class MyRuleFastClosure6 extends
            AbstractRuleFastClosure_3_5_6_7_9 {

        public MyRuleFastClosure6(String name,
                final String database,
                final String focusStore,
                final IConstant<IV> rdfsSubPropertyOf,
                final IConstant<IV> propertyId,
                final Set<IV> set) {

            super(name, database, rdfsSubPropertyOf, propertyId,
                    new MyFastClosure_6_RuleTaskFactory(database,
                            focusStore, rdfsSubPropertyOf, propertyId, set));
        }

    }

    /**
     * Custom rule evaluation overriden to use a hand-built {@link Set}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class MyFastClosure_6_RuleTaskFactory implements IRuleTaskFactory {

        /**
         * 
         */
        private static final long serialVersionUID = 3058590276200820350L;

        final String database;

        final String focusStore;

        final IConstant<IV> rdfsSubPropertyOf;

        final IConstant<IV> propertyId;
        
        final Set<IV> set;
        
        public MyFastClosure_6_RuleTaskFactory(final String database,
                final String focusStore,
                final IConstant<IV> rdfsSubPropertyOf,
                final IConstant<IV> propertyId,
                final Set<IV> set) {

            this.database = database;
            
            this.focusStore = focusStore;
            
            this.rdfsSubPropertyOf = rdfsSubPropertyOf;
            
            this.propertyId = propertyId;
            
            this.set = set;

        }

        public IStepTask newTask(IRule rule, IJoinNexus joinNexus,
                IBuffer<ISolution[]> buffer) {

            return new FastClosureRuleTask(database, focusStore, rule,
                    joinNexus, buffer, /* P, */
                    rdfsSubPropertyOf, propertyId) {

                public Set<IV> getSet() {

                    // the hand-built property set closure.
                    return set;

                }

            };

        }

    }

}
