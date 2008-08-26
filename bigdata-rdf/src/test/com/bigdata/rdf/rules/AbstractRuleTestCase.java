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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.openrdf.model.Statement;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rio.AbstractStatementBuffer.StatementBuffer2;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.store.AbstractTripleStore.Options;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.DefaultEvaluationPlanFactory2;
import com.bigdata.relation.rule.eval.IEvaluationPlanFactory;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;

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
     * Compares two RDF graphs for equality (same statements).
     * <p>
     * Note: This does NOT handle bnodes, which much be treated as variables for
     * RDF semantics.
     * <p>
     * Note: Comparison is performed in terms of the externalized RDF
     * {@link Statement}s rather than {@link SPO}s since different graphs use
     * different lexicons.
     * <p>
     * Note: If the graphs differ in which entailments they are storing in their
     * data and which entailments are backchained then you MUST make them
     * consistent in this regard. You can do this by exporting one or both using
     * {@link #bulkExport(AbstractTripleStore)}, which will cause all
     * entailments to be materialized in the returned {@link TempTripleStore}.
     * 
     * @param expected
     *            One graph.
     * 
     * @param actual
     *            Another graph <strong>with a consistent policy for forward and
     *            backchained entailments</strong>.
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     */
    public static boolean modelsEqual(AbstractTripleStore expected,
            AbstractTripleStore actual) throws Exception {

//        int actualSize = 0;
        int notExpecting = 0;
        int expecting = 0;
        boolean sameStatements1 = true;
        {

            final ICloseableIterator<BigdataStatement> it = notFoundInTarget(actual, expected);

            try {

                while(it.hasNext()) {

                    final BigdataStatement stmt = it.next();

                    sameStatements1 = false;

                    log("Not expecting: " + stmt);
                    
                    notExpecting++;

//                    actualSize++; // count #of statements actually visited.
                    
                }

            } finally {

                it.close();

            }
                        
            log("all the statements in actual in expected? " + sameStatements1);

        }

//        int expectedSize = 0;
        boolean sameStatements2 = true;
        {

            final ICloseableIterator<BigdataStatement> it = notFoundInTarget(expected, actual);
            
            try {

                while(it.hasNext()) {

                    final BigdataStatement stmt = it.next();

                    sameStatements2 = false;

                    log("    Expecting: " + stmt);
                    
                    expecting++;

//                    expectedSize++; // counts statements actually visited.

                }
                
            } finally {
                
                it.close();
                
            }

//          BigdataStatementIterator it = expected.asStatementIterator(expected
//          .getInferenceEngine().backchainIterator(
//                  expected.getAccessPath(NULL, NULL, NULL)));
//
//            try {
//
//                while(it.hasNext()) {
//
//                BigdataStatement stmt = it.next();
//
//                if (!hasStatement(actual,//
//                        (Resource)actual.getValueFactory().asValue(stmt.getSubject()),//
//                        (URI)actual.getValueFactory().asValue(stmt.getPredicate()),//
//                        (Value)actual.getValueFactory().asValue(stmt.getObject()))//
//                        ) {
//
//                    sameStatements2 = false;
//
//                    log("    Expecting: " + stmt);
//                    
//                    expecting++;
//
//                }
//                
//                expectedSize++; // counts statements actually visited.
//
//                }
//                
//            } finally {
//                
//                it.close();
//                
//            }

            log("all the statements in expected in actual? " + sameStatements2);

        }

//        final boolean sameSize = expectedSize == actualSize;
//        
//        log("size of 'expected' repository: " + expectedSize);
//
//        log("size of 'actual'   repository: " + actualSize);

        log("# expected but not found: " + expecting);
        
        log("# not expected but found: " + notExpecting);
        
        return /*sameSize &&*/ sameStatements1 && sameStatements2;

    }

    private static void log(String s) {

        System.err.println(s);

    }

    /**
     * Visits <i>expected</i> {@link BigdataStatement}s not found in <i>actual</i>.
     * 
     * @param expected
     * @param actual
     * 
     * @return An iterator visiting {@link BigdataStatement}s present in
     *         <i>expected</i> but not found in <i>actual</i>.
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
    static protected ICloseableIterator<BigdataStatement> notFoundInTarget(//
            final AbstractTripleStore expected,//
            final AbstractTripleStore actual //
            ) throws InterruptedException, ExecutionException {
        
        /*
         * The source access path is a full scan of the SPO index.
         */
        final IAccessPath<ISPO> expectedAccessPath = expected.getAccessPath(NULL,
                NULL, NULL);
        
        /*
         * Efficiently convert SPOs to BigdataStatements (externalizes
         * statements).
         */
        final BigdataStatementIterator itr2 = expected
                .asStatementIterator(expectedAccessPath.iterator());

        final int capacity = 100000;
        
        final BlockingBuffer<BigdataStatement> buffer = new BlockingBuffer<BigdataStatement>(capacity);
        
        final StatementBuffer2<Statement, BigdataStatement> sb = new StatementBuffer2<Statement, BigdataStatement>(
                actual, true/* readOnly */, capacity) {

            /**
             * Statements not found in [actual] are written on the
             * BlockingBuffer.
             * 
             * @return The #of statements that were not found.
             */
            @Override
            protected int handleProcessedStatements(final BigdataStatement[] a) {
            
                if (log.isInfoEnabled())
                    log.info("Given " + a.length + " statements");
                
                // bulk filter for statements not present in [actual].
                final IChunkedOrderedIterator<ISPO> notFoundItr = actual
                        .bulkFilterStatements(a, a.length, false/* present */);
                
                int nnotFound = 0;
                
                try {
                 
                    while(notFoundItr.hasNext()) {

                        final ISPO notFoundStmt = notFoundItr.next();
                        
                        if (log.isInfoEnabled())
                            log.info("Not found: " + notFoundStmt);
                        
                        buffer.add((BigdataStatement)notFoundStmt);
                        
                        nnotFound++;
                        
                    }
                    
                } finally {
                    
                    notFoundItr.close();
                    
                }

                if (log.isInfoEnabled())
                    log.info("Given " + a.length + " statements, " + nnotFound
                            + " of them were not found");
                
                return nnotFound;
                
            }
            
        };
        
        /**
         * Run task. The task consumes externalized statements from [expected]
         * and writes statements not found in [actual] onto the blocking buffer.
         */
        buffer.setFuture(actual.getExecutorService().submit(new Callable<Void>() {

            public Void call() throws Exception {

                try {
                    
                    while(itr2.hasNext()) {

                        // a statement from the source db.
                        final BigdataStatement stmt = itr2.next();

//                        if (log.isInfoEnabled()) log.info("Source: " + stmt);
                        
                        // add to the buffer.
                        sb.add( stmt );
                        
                    }
                    
                } finally {
                    
                    itr2.close();
                    
                }
                
                /*
                 * Flush everything in the StatementBuffer so that it shows up in
                 * the BlockingBuffer's iterator().
                 */
                
                final long nnotFound = sb.flush();

                if (log.isInfoEnabled())
                    log.info("Flushed: #notFound=" + nnotFound);
                
                return null;

            }

        }));

        /*
         * Return iterator reading "not found" statements from the blocking
         * buffer.
         */

        return buffer.iterator();
        
    }

    /**
     * Exports all statements found in the data and all backchained entailments
     * for the <i>db</i> into a {@link TempTripleStore}. This may be used to
     * compare graphs purely in their data by pre-generation of all backchained
     * entailments.
     * 
     * @param db
     *            The source database.
     * 
     * @return The {@link TempTripleStore}.
     */
    protected TempTripleStore bulkExport(AbstractTripleStore db) {

        final Properties properties = new Properties();
        
        properties.setProperty(Options.ONE_ACCESS_PATH, "true");
        
        properties.setProperty(Options.JUSTIFY, "false");
        
        properties.setProperty(Options.AXIOMS_CLASS,
                NoAxioms.class.getName());
        
        final TempTripleStore tmp = new TempTripleStore(properties);

        final StatementBuffer sb = new StatementBuffer(tmp, 100000/* capacity */);

        final IChunkedOrderedIterator<ISPO> itr1 = new BackchainAccessPath(db,
                tmp.getIndexManager(), db.getAccessPath(NULL, NULL, NULL))
                .iterator();

        final BigdataStatementIterator itr2 = db.asStatementIterator(itr1);

        try {

            while (itr2.hasNext()) {

                final BigdataStatement stmt = itr2.next();

                sb.add(stmt);

            }

        } finally {

            try {
                itr2.close();
                
            } catch (SailException ex) {
                
                throw new RuntimeException(ex);
                
            }

        }

        sb.flush();

        return tmp;

    }

}
