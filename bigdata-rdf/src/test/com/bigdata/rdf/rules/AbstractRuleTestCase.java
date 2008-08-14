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


import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.StatementResolverTask;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTestCase;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.ConcurrentDataLoader.VerifyStatementBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.ChunkedWrappedIterator;
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
            IElementFilter<ISPO> filter, long expectedSolutionCount,
            long expectedMutationCount) throws Exception {

        // Note: Now handled by RDFJoinNexus.
//        /*
//         * Note: Rules run against the committed view.
//         * 
//         * These tests were originally written when rules ran against the
//         * unisolated indices, so a commit is being requested here to ensure
//         * that the test cases all run against a view with access to the data
//         * which they have written on the store.
//         */
//        db.commit();
        
        // dump the database on the console.
        if(log.isInfoEnabled())
            log.info("\ndatabase(before)::\n" + db.dumpStore());

        // run as query.
        {

            final IJoinNexus joinNexus = db.newJoinNexusFactory(
            		RuleContextEnum.HighLevelQuery,
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
            		RuleContextEnum.DatabaseAtOnceClosure,
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
     * Compares two RDF graphs for equality (same statements). This does NOT
     * handle bnodes, which much be treated as variables for RDF semantics. Note
     * that term identifiers are assigned by the lexicon for each database and
     * will differ between databases. Therefore, the comparison is performed in
     * terms of the externalized RDF {@link Statement}s rather than {@link SPO}s.
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

            BigdataStatementIterator it = actual.asStatementIterator(actual
                    .getInferenceEngine().backchainIterator(NULL, NULL, NULL));

            try {

                while(it.hasNext()) {

                    BigdataStatement stmt = it.next();

                    if (!hasStatement(expected,//
                            (Resource)actual.getValueFactory().asValue(stmt.getSubject()),//
                            (URI)actual.getValueFactory().asValue(stmt.getPredicate()),//
                            (Value)actual.getValueFactory().asValue(stmt.getObject()))//
                            ) {

                        sameStatements1 = false;

                        log("Not expecting: " + stmt);
                        
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

            BigdataStatementIterator it = expected.asStatementIterator(expected
                    .getInferenceEngine().backchainIterator(NULL, NULL, NULL));

            try {

                while(it.hasNext()) {

                BigdataStatement stmt = it.next();

                if (!hasStatement(actual,//
                        (Resource)actual.getValueFactory().asValue(stmt.getSubject()),//
                        (URI)actual.getValueFactory().asValue(stmt.getPredicate()),//
                        (Value)actual.getValueFactory().asValue(stmt.getObject()))//
                        ) {

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

    static private boolean hasStatement(AbstractTripleStore database,
            Resource s, URI p, Value o) {

        if (RDF.TYPE.equals(p) && RDFS.RESOURCE.equals(o)) {

            if (database.getTermId(s) != NULL) {

                return true;

            }

        }

        return database.hasStatement(s, p, o);

    }

    /**
     * Visits {@link SPO}s not found in the target database.
     * 
     * @param src
     * @param tgt
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     * 
     * @todo rewrite the {@link StatementVerifier} and
     *       {@link VerifyStatementBuffer} to use the same logic.
     */
    protected Iterator<ISPO> notFoundInTarget(//
            final AbstractTripleStore src,//
            final AbstractTripleStore tgt //
            ) throws InterruptedException, ExecutionException {
        
        /*
         * Visit all SPOs in the source, including backchained
         * inferences.
         */
        final IChunkedOrderedIterator<ISPO> itr1 = src
                .getInferenceEngine().backchainIterator(NULL, NULL,
                        NULL);

        /*
         * Efficiently convert SPOs to BigdataStatements (externalizes
         * statements).
         */
        final BigdataStatementIterator itr2 = src.asStatementIterator(itr1);

        /*
         * Buffer size (#of statements). 
         */
        final int capacity = 10000; // @todo large capacity for bulk loads.
        
        /*
         * Read only since we do not want to write on the lexicon or the
         * statement indices, just resolve what already exists and look for
         * missing statements.
         */
        final boolean readOnly = true;
        
        /*
         * FIXME Change the striterator heirarchy into an implementation
         * heirarchy (StriteratorImpl) for co-varying generics and a use
         * hierarchy (IStriterator and Striterator) in which only the element
         * type is generic. This should be significantly easier to use and
         * understand.
         */
        
        /*
         * Task that will efficiently convert to BigdataStatements using the
         * lexicon for the target database.
         */
        final StatementResolverTask<ISPO> task = new StatementResolverTask<ISPO>(
                (Iterator<BigdataStatement>) itr2, readOnly, capacity, tgt) {

            /** bulk filter for SPOs NOT FOUND in the target. */
            @Override
            protected Iterator<ISPO> getOutputIterator() {

                return tgt.bulkFilterStatements(
                        new ChunkedWrappedIterator<ISPO>(this.iterator()),
                        false/*present*/);

            }
            
        };

        /*
         * Run the task.
         * 
         * Note: We do not obtain the Future. If an error occurs, then we will
         * notice it when the converting iterator is closed.
         */
        tgt.getExecutorService().submit(task);

        // visits SPOs NOT FOUND in the target.
        return task.iterator();

    }

}
