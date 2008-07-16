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

import org.openrdf.model.Statement;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.relation.accesspath.EmptyAccessPath;
import com.bigdata.relation.accesspath.IChunkedOrderedIterator;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;

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

    protected void applyRule(AbstractTripleStore db, Rule rule,
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
    protected void applyRule(AbstractTripleStore db, Rule rule,
            IElementFilter<SPO> filter, long expectedSolutionCount,
            long expectedMutationCount) throws Exception {

        /*
         * Note: Rules run against the committed view.
         * 
         * These tests were originally written when rules ran against the
         * unisolated indices, so a commit is being requested here to ensure
         * that the test cases all run against a view with access to the data
         * which they have written on the store.
         */
        db.commit();
        
        // dump the database on the console.
        if(log.isInfoEnabled())
            log.info("\ndatabase(before)::\n" + db.dumpStore());

        // run as query.
        {

            final IJoinNexus joinNexus = db.newJoinNexusFactory(
                    ActionEnum.Query, IJoinNexus.ALL, filter).newInstance(
                    db.getIndexManager());

            long n = 0;
            
            final IChunkedOrderedIterator<ISolution> itr = joinNexus.runQuery(rule);
            
            try {

                while(itr.hasNext()) {

                    final ISolution[] chunk = itr.nextChunk();
                    
                    for(ISolution solution : chunk) {
                    
                        n++;
                        
                        log.info("#"+n+" : "+solution.toString());
                        
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
                    ActionEnum.Insert, IJoinNexus.ALL, filter).newInstance(
                    db.getIndexManager());

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
     * Compares two RDF graphs for equality (same statements) - does NOT handle
     * bnodes, which much be treated as variables for RDF semantics.
     * 
     * @param expected
     * 
     * @param actual
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     * 
     * FIXME refactor to {@link AbstractTestCase} and make sure that the same
     * logic is not present in the rest of the test suite.
     */
    public static boolean modelsEqual(AbstractTripleStore expected,
            AbstractTripleStore actual) throws SailException {

        int actualSize = 0;
        int notExpecting = 0;
        int expecting = 0;
        boolean sameStatements1 = true;
        {

            BigdataStatementIterator it = actual.getStatements(null, null, null);

            try {

                while(it.hasNext()) {

                    Statement stmt = it.next();

                    if (!expected.hasStatement(stmt.getSubject(), stmt
                            .getPredicate(), stmt.getObject())) {

                        sameStatements1 = false;

                        log("Not expecting: " + stmt);
                        
                        if (expected.getAccessPath(stmt.getSubject(), stmt
                                .getPredicate(), stmt.getObject()) instanceof EmptyAccessPath) {
                        	
                        	log("empty access path");
                        	
                        }
                        
                        notExpecting++;

                    }

                    actualSize++; // count #of statements actually visited.
                    
                }

            } finally {

                it.close();

            }
            
            log("all the statements in actual in expected? " + sameStatements1);

        }

        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            BigdataStatementIterator it = expected.getStatements(null, null, null);

            try {

                while(it.hasNext()) {

                Statement stmt = it.next();

                if (!actual.hasStatement(stmt.getSubject(),
                        stmt.getPredicate(), stmt.getObject())) {

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);
                    
                    expecting++;

                }
                
                expectedSize++; // counts statements actually visited.

                }
                
            } finally {
                
                it.close();
                
            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

        final boolean sameSize = expectedSize == actualSize;
        
        log("size of 'expected' repository: " + expectedSize);

        log("size of 'actual'   repository: " + actualSize);

        log("# expected but not found: " + expecting);
        
        log("# not expected but found: " + notExpecting);
        
        return sameSize && sameStatements1 && sameStatements2;

    }

    private static void log(String s) {

        System.err.println(s);

    }

}
