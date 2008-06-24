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
 * Created on Jun 24, 2008
 */

package com.bigdata.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

/**
 * Utility class for {@link IProgram}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ProgramUtility {

    protected static final Logger log = Logger.getLogger(ProgramUtility.class);
    
    public static transient final ProgramUtility INSTANCE = new ProgramUtility();

    private ProgramUtility() {

    }

    public void execute(IProgram program, ExecutorService service,
            IBuffer<IBindingSet> buffer) throws InterruptedException,
            ExecutionException {

        final List<Callable<Object>> tasks = newTasks(program, buffer);

        if (program.isParallel()) {

            runParallel(service, tasks);

        } else {

            runSequential(service, tasks);

        }

    }

    /**
     * Builds a set of tasks for the program.
     * 
     * @param buffer
     * 
     * @return
     * 
     * @todo handle sub-programs (vs just rules).
     * 
     * FIXME handle closure using
     * {@link #fixPoint(IProgram, ExecutorService, IBuffer)}.
     * 
     * @todo do we really want the buffer as a parameter here?
     */
    protected List<Callable<Object>> newTasks(IProgram program, IBuffer<IBindingSet> buffer) {

        final List<Callable<Object>> tasks = new ArrayList<Callable<Object>>(program.stepCount());

        final Iterator<IProgram> itr = program.steps();

        while (itr.hasNext()) {

            // @todo handle sub-programs.
            final Rule rule = (Rule) itr.next();

            final RuleState ruleState = newRuleState(rule);

            final IRuleEvaluator task = new LocalNestedSubqueryEvaluator(
                    ruleState, buffer);

            tasks.add(task);

        }

        return tasks;

    }
    
    /**
     * Run program steps in parallel.
     * 
     * @param service
     * @param tasks
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected void runParallel(ExecutorService service,
            List<Callable<Object>> tasks) throws InterruptedException,
            ExecutionException {

        // submit tasks and await their completion.
        final List<Future<Object>> futures = service.invokeAll(tasks);

        // verify no problems with tasks.
        for (Future<Object> f : futures) {

            f.get();

        }

    }

    /**
     * Run program steps in sequence.
     * 
     * @param service
     * @param tasks
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected void runSequential(ExecutorService service,
            List<Callable<Object>> tasks) throws InterruptedException,
            ExecutionException {

        final Iterator<Callable<Object>> itr = tasks.iterator();

        while (itr.hasNext()) {

            final Callable<Object> task = itr.next();

            // submit and wait for the future.
            service.submit(task).get();

        }

    }
    
    /**
     * Computes the "fixed point" of a program.
     * <p>
     * The general approach is a series of rounds in which each rule is applied
     * in turn (either sequentially or in parallel, depending on the program).
     * Solutions computed for each rule in each round written onto the relation
     * for the head of that rule. The process halts when no new solutions are
     * computed in a given round.
     * 
     * @param closureStats
     *            Used to aggregate statistics across the fixed point for a
     *            series of rule rules (the fast closure method does this).
     * @param program
     *            The program to be executed.
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
     * 
     * FIXME There is going to be a buffer per rule head. For RDF, those are
     * always the same relation and the buffer will be the same object and rules
     * running in parallel will write on the shared buffer in parallel, but that
     * is not really required.
     * 
     * @todo Does it make sense for the sub-programs of a closure to themselves
     *       allow closure or should they be required to be a simple set of
     *       rules to be executed it parallel?
     * 
     * @todo Should the rule execution should report the #of elements written on
     *       the buffer. If the buffer used to write on the relation, then
     *       should it report the #of elements inserted or removed from the
     *       relation? If we do that, then the fixed point can be determined
     *       when no more elements are written on (any of the) head relation(s).
     * 
     * @throws ExecutionException
     * @throws InterruptedException
     */
//    * @return Some statistics about the fixed point computation.
    protected void //ClosureStats
    fixPoint(//ClosureStats closureStats,
            IProgram program,//
            ExecutorService service,//
            IBuffer<IBindingSet> buffer//
            ) throws InterruptedException, ExecutionException {
        
        long firstStatementCount = -1L;
        long lastStatementCount = -1L;

        final long begin = System.currentTimeMillis();

        final IProgram[] steps = program.toArray();

        /*
         * FIXME After each round we need to be reading from the post-update
         * state the relation(s) on which the rules are writing. (It is an error
         * if the rules are not writing on at least one relation.)  This assumes
         * that all of the rules are writing on the relation specified for the
         * head of the first rule.
         */
        final IRelation sink = ((Rule)steps[0]).getHead().getRelation();
        
        /*
         * FIXME We need an exact count to detect the fixed point since it is
         * otherwise possible that a deleted tuple would be overwritten by a
         * computed entailment but that the count would not change. Since this
         * is so (potentially) expensive it would be best to get back the #of
         * tuples actually written on the index from each rule in each round. If
         * no rules in a given round caused a tuple to be written, then we are
         * at the fixed point. (This assumes that you are following the
         * IRelation contract with respect to not overwriting tuples with the
         * same key and value.)
         */
        final boolean exact = true;
        
        int round = 0;

        while (true) {

            final long numEntailmentsBefore = sink.getElementCount(exact);

            if (round == 0) {

                firstStatementCount = numEntailmentsBefore;
             
                if (log.isDebugEnabled())
                    log.debug("Closing kb with " + firstStatementCount + " statements");

            }

            for (IProgram step : steps) {

//                final RuleStats stats =
                execute(step, service, buffer);
//                    apply(rule, focusStore, database, buffer, service);
                
// closureStats.add(stats);
//                
//                if (log.isDebugEnabled() || true) {
//
//                    log.debug("round# " + round + ":" + stats);
//                    
//                }
//                
//                closureStats.nentailments += stats.numComputed;
//                
//                closureStats.elapsed += stats.elapsed;
                
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

            final long numEntailmentsAfter = sink.getElementCount(exact);
            
            if ( numEntailmentsBefore == numEntailmentsAfter ) {
                
                // This is the fixed point.

                lastStatementCount = numEntailmentsAfter;

                break;
                
            }

//            if(log.isInfoEnabled()) {
//
//                log.info("round #"+round+"\n"+closureStats.toString());
//                
//            }

            round++;
            
        }

        final long elapsed = System.currentTimeMillis() - begin;

        if (log.isInfoEnabled()) {

//            log.info("\n"+closureStats.toString());

            final long inferenceCount = lastStatementCount - firstStatementCount;
            
            // names of the rules that we ran.
            final String names; 
            {
            
                StringBuilder sb = new StringBuilder();
                
                sb.append("[");
                
                int i = 0;
                
                for(IProgram step : steps ) {
                    
                    if (i > 0)
                        sb.append(",");

                    sb.append(step.getName());
                    
                }

                sb.append("]");

                names = sb.toString();
                
            }
            
            log.info("\nComputed fixed point for "+steps.length+" steps in "
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

        return;// closureStats;

    }

    /**
     * Factory for {@link RuleState} instances.
     * 
     * @param rule
     *            The {@link Rule}.
     */
    public RuleState newRuleState(Rule rule) {
        
        return new RuleState( rule );
        
    }
    
}
