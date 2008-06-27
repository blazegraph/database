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
 * Created on Jun 25, 2008
 */

package com.bigdata.join.rdf;

import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.join.AbstractSolutionBuffer;
import com.bigdata.join.ActionEnum;
import com.bigdata.join.ArrayBindingSet;
import com.bigdata.join.BlockingBuffer;
import com.bigdata.join.Constant;
import com.bigdata.join.DefaultEvaluationPlan;
import com.bigdata.join.EmptyProgramTask;
import com.bigdata.join.IBindingSet;
import com.bigdata.join.IBlockingBuffer;
import com.bigdata.join.IBuffer;
import com.bigdata.join.IConstant;
import com.bigdata.join.IEvaluationPlan;
import com.bigdata.join.IJoinNexus;
import com.bigdata.join.IMutableRelation;
import com.bigdata.join.IPredicate;
import com.bigdata.join.IProgram;
import com.bigdata.join.IProgramTask;
import com.bigdata.join.IRelationLocator;
import com.bigdata.join.IRule;
import com.bigdata.join.ISolution;
import com.bigdata.join.IVariable;
import com.bigdata.join.IVariableOrConstant;
import com.bigdata.join.LocalProgramTask;
import com.bigdata.join.RuleState;
import com.bigdata.join.Solution;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.ITx;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.LocalDataServiceFederation;

/**
 * Encasulates everything required to execute JOINs for the {@link SPORelation}
 * when running with a single embedded {@link DataService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOJoinNexus implements IJoinNexus {

    protected static Logger log = Logger.getLogger(SPOJoinNexus.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1151635508669768925L;

    private final IBigdataClient client;
    
    private final boolean elementOnly;
    
    private final IRelationLocator<SPO> relationLocator;
    
    public final boolean isElementOnly() {
        
        return elementOnly;
        
    }

    /**
     * 
     * @param elementOnly
     */
    public SPOJoinNexus(IBigdataClient client, boolean elementOnly,
            IRelationLocator<SPO> relationLocator) {

        if (client == null)
            throw new IllegalArgumentException();

        if (relationLocator == null)
            throw new IllegalArgumentException();

        this.client = client;
        
        this.elementOnly = elementOnly;

        this.relationLocator = relationLocator;
        
    }
    
    @SuppressWarnings("unchecked")
    public void copyValues(Object e, IPredicate predicate, IBindingSet bindingSet ) {

        if (e == null)
            throw new IllegalArgumentException();
        
        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final SPO spo = (SPO) e;
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>)predicate;
        
        {

            final IVariableOrConstant<Long> t = pred.get(0);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.s));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(1);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.p));
                
            }

        }

        {

            final IVariableOrConstant<Long> t = pred.get(2);
            
            if(t.isVar()) {

                bindingSet.set((IVariable<Long>) t, new Constant<Long>(spo.o));
                
            }

        }
        
    }

    @SuppressWarnings("unchecked")
    public SPO newElement(IPredicate predicate, IBindingSet bindingSet) {

        if (predicate == null)
            throw new IllegalArgumentException();
        
        if (bindingSet == null)
            throw new IllegalArgumentException();
        
        final IPredicate<ISPO> pred = (IPredicate<ISPO>) predicate;

        final long s = asBound(pred, 0, bindingSet);

        final long p = asBound(pred, 1, bindingSet);

        final long o = asBound(pred, 2, bindingSet);

        final SPO spo = new SPO(s, p, o, StatementEnum.Inferred);
        
        if(log.isDebugEnabled()) {
            
            log.debug(spo.toString());
            
        }
        
        return spo;
        
    }

    /**
     * Extract the bound value from the predicate. When the predicate is not
     * bound at that index, then extract its binding from the binding set.
     * 
     * @param pred
     *            The predicate.
     * @param index
     *            The index into that predicate.
     * @param bindingSet
     *            The binding set.
     *            
     * @return The bound value.
     */
    private long asBound(IPredicate<ISPO> pred, int index, IBindingSet bindingSet) {

        final IVariableOrConstant<Long> t = pred.get(index);

        final IConstant<Long> c;
        if(t.isVar()) {
            
            c = bindingSet.get((IVariable) t);
            
        } else {
            
            c = (IConstant<Long>)t;
            
        }

        return c.get().longValue();

    }
    
    public ISolution<SPO> newSolution(IRule rule, IBindingSet bindingSet) {

        final SPO spo = newElement(rule.getHead(), bindingSet);

        final Solution<SPO> solution;
        
        if (elementOnly) {

            solution = new Solution<SPO>(spo);

        } else {

            solution = new Solution<SPO>(spo, rule, bindingSet.clone());
            
        }
        
        if(log.isDebugEnabled()) {
            
            log.debug(solution.toString());
            
        }
        
        return solution;

    }

    public IBindingSet newBindingSet(IRule rule) {

        return new ArrayBindingSet(rule.getVariableCount());
        
    }

    public RuleState newRuleState(IRule rule) {
    
        return new RuleState( rule, this );
        
    }

    public IRelationLocator<SPO> getRelationLocator() {
        
        return relationLocator;
        
    }

    public IEvaluationPlan newEvaluationPlan(IRule rule) {
        
        return new DefaultEvaluationPlan(this, rule);
        
    }

    public IBlockingBuffer<ISolution> newQueryBuffer() {

        return new BlockingBuffer<ISolution>();
        
    }
    
    /**
     * The default buffer capacity.
     */
    private final int DEFAULT_BUFFER_CAPACITY = 10000;
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newInsertBuffer(IMutableRelation relation) {

        return new AbstractSolutionBuffer.InsertSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation);

    }
    
    @SuppressWarnings("unchecked")
    public IBuffer<ISolution> newDeleteBuffer(IMutableRelation relation) {

        return new AbstractSolutionBuffer.DeleteSolutionBuffer(
                DEFAULT_BUFFER_CAPACITY, relation);

    }

    /**
     * @deprecated by {@link #runProgram(ActionEnum, IProgram)}
     */
    public IProgramTask newProgramTask(final ActionEnum action,
            final IProgram program) {

        if (!program.isRule() && program.stepCount() == 0) {

            log.warn("Empty program");

            return new EmptyProgramTask(action, program);

        }

        final IProgramTask task = new LocalProgramTask(action, program, this);
        
        return task;

    }

    /**
     * FIXME create two variants : one for query and one for insert/update .
     * That will make the API cleaner for the caller.
     */
    public Object runProgram(ActionEnum action, IProgram program) throws Exception {

        if (!program.isRule() && program.stepCount() == 0) {

            log.warn("Empty program");

            return new EmptyProgramTask(action, program).call();

        }

        //      final IProgramTask innerTask = newProgramTask(action, program);

        final IBigdataFederation fed = client.getFederation();
        
        final IProgramTask innerTask;

        /*
         * FIXME Can't run yet on the data service. both a bug and need access
         * to IBigdataClient there!
         */
        if (false && fed instanceof LocalDataServiceFederation) {
            
            /*
             * This variant is submitted and executes on the DataService (fast).
             * This can only be done if all indices for the relation(s) are
             * located on the SAME data service.
             * 
             * This is always true for LDS.
             * 
             * It MAY be true for other federations. It will certainly be true
             * for an EDS with only one DataService.
             * 
             * @todo improve decision making here (cover more cases).
             * 
             * @todo handle a purely local Journal.
             */
            
            final DataService dataService = ((LocalDataServiceFederation)fed).getDataService();

            innerTask = new LocalProgramTask(action, program, this);
                        
            log.info("Will run on data service.");
            
            final IConcurrencyManager concurrencyManager = dataService.getConcurrencyManager();
            
            /*
             * Note: The index names must be gathered from each relation on
             * which the task will write so that they can be declared. We can't
             * just pick and choose using the access paths since we do not know
             * how the propagation of bindings will effect access path selection
             * so we need a lock on all of the indices before the task can run
             * (at least, before it can run if it is a writer - no locks are
             * required for query).
             */
            
            final String[] resource;
            final long timestamp;
            if(action.isMutation()) {

                timestamp = ITx.UNISOLATED;

                /*
                 * FIXME This is going to require an API to disclose the index
                 * names for the IRelation.
                 * 
                 * FIXME This should not be an unisolated task. The writing the
                 * mutations on the relation needs to be unisolated, but
                 * computing the solutions can be a historical read.
                 * 
                 * FIXME For a fixed point computation, we need to advance the
                 * commit time after each round.
                 */
                resource = new String[]{"test.SPO","test.POS","test.OSP"};

                log.info("Will submit unisolated task");
                
            } else {
                
                // FIXME negative timestamp @issue
                timestamp = - dataService.getResourceManager().getLiveJournal()
                        .getCommitRecord().getTimestamp();

                resource = new String[] {};

                log.info("Will submit read-only task");

            }
            
            final AbstractTask task = new AbstractTask(concurrencyManager, timestamp,
                    resource) {

                @Override
                protected Object doTask() throws Exception {
                    
                    log.info("Execution the inner task");
                    
                    return innerTask.call();
                    
                }
                
            };

            log.info("Submitting task to the data service.");

            final Future<Object> future = concurrencyManager.submit( task );
            
            return future.get();
            
        } else {
            
            /*
             * This variant will execute using IClientIndex (slow). It needs to
             * be used if the federation is distributed since we can't assume
             * that all indices are on the same data service.
             */

            log.info("Running inner task in caller's thread.");

            innerTask = new LocalProgramTask(action, program, this, client
                    .getFederation().getThreadPool());
            
            return innerTask.call();

        }

    }
    
}
