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



import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleTestCase extends AbstractInferenceEngineTestCase {
    
    /**
     * 
     */
    public AbstractRuleTestCase() {
    }

    /**
     * @param name
     */
    public AbstractRuleTestCase(String name) {
        super(name);
    }

    protected void applyRule(AbstractTripleStore db, IRule rule,
            long expectedSolutionCount, long expectedMutationCount)
            throws Exception {

        applyRule(db, rule, null/* filter */, expectedSolutionCount,
                expectedMutationCount);

    }
    
    /**
     * Applies the rule, copies the new entailments into the store and checks
     * the expected #of inferences computed and new statements copied into the
     * store.
     * <p>
     * Invoke as <code>applyRule( store.{rule}, ..., ... )</code>
     * 
     * @param rule
     *            The rule, which must be one of those found on {@link #store}
     *            or otherwise configured so as to run with the {@link #store}
     *            instance.
     * 
     * @param expectedSolutionCount
     *            The #of entailments that should be computed by the rule
     *            (tested during Query) or <code>-1</code> if the #of
     *            solutions that will be computed is not known.
     * 
     * @param expectedMutationCount
     *            The #of new elements that should have been inserted into the
     *            db by the rule.
     */
    protected void applyRule(AbstractTripleStore db, IRule rule,
            IElementFilter<ISPO> filter, long expectedSolutionCount,
            long expectedMutationCount) throws Exception {

        // dump the database on the console.
        if(log.isInfoEnabled())
            log.info("\ndatabase(before)::\n" + db.dumpStore());

//        final IElementFilter filter = null;
        final boolean justify = false;
        final boolean backchain = false;
        final IEvaluationPlanFactory planFactory = DefaultEvaluationPlanFactory2.INSTANCE;
        
        // run as query.
        {

            final IJoinNexus joinNexus = db.newJoinNexusFactory(
                    RuleContextEnum.HighLevelQuery, ActionEnum.Query,
                    IJoinNexus.ALL, filter, justify, backchain, planFactory)
                    .newInstance(db.getIndexManager());

            long n = 0;
            
            final IChunkedOrderedIterator<ISolution> itr = joinNexus.runQuery(rule);
            
            try {

                while(itr.hasNext()) {

                    final ISolution[] chunk = itr.nextChunk();
                    
                    for(ISolution solution : chunk) {
                    
                        n++;

                        if (log.isInfoEnabled())
                            log.info("#" + n + " : " + solution.toString());
                        
                    }
                    
                }

            } finally {

                itr.close();
                
            }

            if (log.isInfoEnabled())
                log.info("Computed " + n + " solutions: " + rule.getName() + " :: " + rule);
            
            if (expectedSolutionCount != -1L) {

                assertEquals("solutionCount(Query)", expectedSolutionCount, n);

            }
            
        }

        // run as insert.
        {

            final IJoinNexus joinNexus = db.newJoinNexusFactory(
                    RuleContextEnum.DatabaseAtOnceClosure, ActionEnum.Insert,
                    IJoinNexus.ALL, filter, justify, backchain, planFactory)
                    .newInstance(db.getIndexManager());

            final long actualMutationCount = joinNexus.runMutation(rule);

            if (log.isInfoEnabled())
                log.info("Inserted " + actualMutationCount + " elements : "
                        + rule.getName() + " :: " + rule);

            assertEquals("mutationCount(Insert)", expectedMutationCount,
                    actualMutationCount);

        }
                
        // dump the database.
        if(log.isInfoEnabled())
            log.info("\ndatabase(after)::\n" + db.dumpStore());
        
    }

    /**
     * Verifies the the iterator visits {@link ISolution}s have the expected
     * {@link IBindingSet}s when those bindings may occur in any order.
     * 
     * @param itr
     *            The iterator.
     * @param expected
     *            The expected {@link IBindingSet}s.
     */
    protected void assertSameSolutionsAnyOrder(final IChunkedOrderedIterator<ISolution> itr,
            final IBindingSet[] expected) {

        if (itr == null)
            throw new IllegalArgumentException();

        if (expected == null)
            throw new IllegalArgumentException();

        try {

            int n = 0;
            
            while(itr.hasNext()) {
                
                final IBindingSet actual = itr.next().getBindingSet();

                assertNotNull("bindings not requested?", actual);
                
                if(!findAndClear(actual, expected)) {
                    
                    fail("Not found: ndone=" + n + ", bindingSet=" + actual);
                    
                }
                
                n++;
                
            }
            
            if(log.isInfoEnabled())
                log.info("Matched "+n+" binding sets");
            
            // verify correct #of solutions identified.
            assertEquals("#of solutions", n, expected.length);
            
        } finally {
            
            itr.close();
            
        }
        
    }

    /**
     * Locates an {@link IBindingSet} equal to the given one in the array,
     * clears that entry from the array, and returns <code>true</code>. If
     * none is found, then returns <code>false</code>.
     * 
     * @param actual
     *            The binding set.
     * @param expected
     *            An array of expected binding sets.
     *            
     * @return <code>true</code> if <i>actual</i> was matched in the expected
     *         binding set array.
     */
    private boolean findAndClear(IBindingSet actual, IBindingSet[] expected) {
        
        for (int i = 0; i < expected.length; i++) {
            
            IBindingSet tmp = expected[i];

            if (tmp == null)
                continue;
            
            if(actual.equals(tmp)) {

                // found.
                
                expected[i] = null;
                
                if (log.isInfoEnabled())
                    log.info("Matched: index=" + i + ", bindingSet=" + actual);
                

                return true;

            }

        }

        log.warn("No match: bindingSet=" + actual);
        
        // not found
        return false;
        
    }
    
}
