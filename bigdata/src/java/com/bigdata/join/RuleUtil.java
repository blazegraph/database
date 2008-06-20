/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Jun 20, 2008
 */

package com.bigdata.join;

import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.bigdata.service.IBigdataFederation;

/**
 * Utility class for execution of rules.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RuleUtil {

    protected static final Logger log = Logger.getLogger(RuleUtil.class);
    
    /**
     * Map N executions of rule over the terms in the tail. In each pass term[i]
     * will read from <i>focusStore</i> and the other term(s) will read from a
     * fused view of the <i>focusStore</i> and the <i>database</i>.
     * <p>
     * Within each pass, the decision on the order in which the terms will be
     * evaluated is made based on the rangeCount() for the {@link IAccessPath}
     * selected for that term.
     * <p>
     * The N passes themselves are executed concurrently.
     * 
     * @param rule
     *            The rule to execute.
     * @param focusStore
     *            An optional {@link AbstractTripleStore}.
     * @param database
     *            The persistent database.
     * @param service
     *            The service that will be used to execute the rules. The
     *            service may be used for both mapping the rules in parallel and
     *            for parallelism of subqueries execution within rules. Normally
     *            you would pass in {@link IBigdataFederation#getThreadPool()}.
     * @param buffer
     *            The rules will write the entailments (and optionally the
     *            justifications) on the buffer.
     * 
     * @todo We can in fact run the variations of the rule in parallel using an
     *       {@link ExecutorService}.
     *       <p>
     *       The {@link AbstractArrayBuffer} and then {@link BlockingBuffer}
     *       thread-safe so that the N passes may be concurrent and they all
     *       write onto the same buffer, hence their union is automatically
     *       visible in the iterator wrapping that buffer.
     *       <p>
     *       The {@link RuleState} bindings, order, and dependency graph all all
     *       per-thread.
     *       <p>
     *       {@link RuleStats} is per-{@link RuleState) and
     *       {@link RuleStats#add(RuleStats)} is synchronized.
     */
    static public RuleStats apply(Rule rule, IAccessPathFactory focusStore,
            IAccessPathFactory database, IBuffer<IBindingSet> buffer,
            ExecutorService service, IRuleEvaluator evaluator) { 

        if (rule == null)
            throw new IllegalArgumentException();

        if (database == null)
            throw new IllegalArgumentException();

        if (buffer == null)
            throw new IllegalArgumentException();

        if (evaluator == null)
            throw new IllegalArgumentException();

        if (focusStore == null) {

            /*
             * Just close the database.
             */

            RuleState state = new RuleState(rule, 0/* focusIndex */,
                    focusStore, database, buffer);

            evaluator.apply(state);

            return state.stats;

        }
        
        /*
         * When focusStore != null we need to run the rule N times, where N is
         * the #of predicates in the body. In each pass we choose body[i] as the
         * focusIndex - the predicate that will read from the [focusStore]. All
         * other predicates will read from the fused view of the [focusStore]
         * and the [database].
         * 
         * Note: when the rule has a single predicate in the tail, the predicate
         * is only run against [focusStore] rather than [datbase] or [focusStore +
         * database].
         * 
         * Note: all of these passes write on the same buffer. This has the same
         * effect as a UNION over the entailments of the individual passes.
         * 
         * @todo run the N passes in parallel.
         */

        // statistics aggregated across the rule variants that we will run.
        final RuleStats stats = new RuleStats(rule);

        final int tailCount = rule.getTailCount();
        
        final RuleState[] state = new RuleState[tailCount];
        
        for (int i = 0; i < tailCount; i++) {

            state[i] = new RuleState(rule, i/* focusIndex */, focusStore,
                    database, buffer);

            evaluator.apply( state[i] );
            
            stats.add( state[i].stats );
            
        }
        
        return stats;
        
    }

    /**
     * An abstraction that can be evaluated to determine the size of the result
     * set. This is invoked before each pass when computing the fixed point of
     * some set of rules. The fixed point is recognized when the size of the
     * result set as reported by this interface.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IResultSetSize {

        /**
         * The size of the result set.
         */
        public long size();
        
    }
    
    /**
     * Computes the "fixed point" of a specified rule set, the persistent
     * database, and an optional "focus" data set using set-at-a-time
     * processing.
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * to all data in turn. Entailments computed in each round are fed back into
     * either the "focusStore" or the database (depending on how things are
     * setup) so that derived entailments may be computed in a succession of
     * rounds. The process halts when no new entailments are computed in a given
     * round.
     * <p>
     * Note: When loading a new data set into the database, the "focusStore"
     * should contain the statements that were read from the data source, e.g.,
     * some RDF/XML file and the "buffer" should be configured to write on the
     * "database".
     * 
     * @param closureStats
     *            Used to aggregate statistics across the fixed point for a
     *            series of rule rules (the fast closure method does this).
     * @param rules
     *            The rules to be executed.
     * @param justify
     *            True iff justifications should be generated (use only when
     *            entailments are being written into the database and then iff
     *            the truth maintenance strategy requires justification chains).
     * @param focusStore
     *            An optional store containing statements. This is typically
     *            used in one of two ways: (1) incremental loading of new data
     *            into the database; and (2) collecting entailments of
     *            statements that are being removed during truth maintenance.
     *            When <code>null</code> the closure of the <i>database</i>
     *            will be performed in the absence of any new information.
     * @param database
     *            The persistent database.
     * @param buffer
     *            Used to buffer generated entailments so that we can do
     *            efficient ordered writes on the statement indices. The buffer
     *            MUST write which ever of the optional <i>focusStore</i> or
     *            the <i>database</i> you want to fix point. The buffer is
     *            flushed periodically during processing and will be empty when
     *            this method returns. All entailments will be in whichever
     *            store the buffer was configured to write upon.
     * @param resultSetSize
     *            An object that is used to decide when the result set (whatever
     *            the buffer writes on) has reached a fixed point.
     * @param service
     *            The service that will be used to execute the rules.
     * @param evaluator
     *            The object that knows how to evaluate the rules.
     * 
     * @return Some statistics about the fixed point computation.
     */
    static public ClosureStats fixedPoint(ClosureStats closureStats,
            Rule[] rules, IAccessPathFactory focusStore,
            IAccessPathFactory database, IBuffer<IBindingSet> buffer,
            IResultSetSize resultSetSize,
            ExecutorService service,
            IRuleEvaluator evaluator) {

        final int nrules = rules.length;

        /*
         * We will fix point whichever store the buffer is writing on.
         */
        
        long firstStatementCount = -1L;
        long lastStatementCount = -1L;

        final long begin = System.currentTimeMillis();

        int round = 0;

        while (true) {

            final long numEntailmentsBefore = resultSetSize.size();

            if (round == 0) {

                firstStatementCount = numEntailmentsBefore;
             
                if (log.isDebugEnabled())
                    log.debug("Closing kb with " + firstStatementCount + " statements");

            }
            
            for (int i = 0; i < nrules; i++) {

                final Rule rule = rules[i];

                final RuleStats stats = apply(rule, focusStore, database,
                        buffer, service, evaluator);
                
                closureStats.add(stats);
                
                if (log.isDebugEnabled() || true) {

                    log.debug("round# " + round + ":" + stats);
                    
                }
                
                closureStats.nentailments += stats.numComputed;
                
                closureStats.elapsed += stats.elapsed;
                
            }

            /*
             * Flush the statements in the buffer
             * 
             * @todo When should we flush the buffer? In between each pass in
             * apply, after apply, or after the round? Each of these is
             * "correct" but there may be performance tradeoffs. Deferring the
             * flush can increase batch size and index write performance. If any
             * rule generates an entailment, then we are going to do another
             * round anyway so maybe it is best to defer to the end of the
             * round?
             * 
             * FIXME each round can use a historical read from the timestamp
             * associated with the commit point of the prior round. in an
             * extended transaction model those could be "save points" such
             * that the total result was either committed or aborted.  If a
             * transaction is used, then the closure actually takes place within
             * the transaction so the commit for the tx is always atomic and the
             * rounds in which we compute the closure are "committed" against the
             * tx's write set (either for the focus store or for the database if
             * we are doing database at once closure).
             */
            buffer.flush();

            final long numEntailmentsAfter = resultSetSize.size();
            
            if ( numEntailmentsBefore == numEntailmentsAfter ) {
                
                // This is the fixed point.

                lastStatementCount = numEntailmentsAfter;

                break;
                
            }

            if(log.isInfoEnabled()) {

                log.info("round #"+round+"\n"+closureStats.toString());
                
            }

            round++;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled()) {

            log.info("\n"+closureStats.toString());

            final long inferenceCount = lastStatementCount - firstStatementCount;
            
            // names of the rules that we ran.
            final String names; 
            {
            
                StringBuilder sb = new StringBuilder();
                
                sb.append("[");
                
                for(int i=0; i<rules.length; i++) {
                    
                    if(i>0) sb.append(",");

                    sb.append(rules[i].getName());
                    
                }

                sb.append("]");

                names = sb.toString();
                
            }
            
            log.info("\nComputed closure of "+rules.length+" rules in "
                            + (round+1) + " rounds and "
                            + elapsed
                            + "ms yeilding "
                            + lastStatementCount
                            + " statements total, "
                            + (inferenceCount)
                            + " inferences"
                            + ", entailmentsPerSec="
                            + (elapsed == 0 ? "N/A" : ""
                            + ((long) (inferenceCount * 1000d) / elapsed)));

            log.info("Rules: "+names);

//            Collection<RuleStats> ruleStats = closureStats.getRuleStats();
//            
//            for (RuleStats tmp : ruleStats) {
//                
//                log.info(tmp.toString());
//                
//            }
                        
        }

        return closureStats;

    }

}
