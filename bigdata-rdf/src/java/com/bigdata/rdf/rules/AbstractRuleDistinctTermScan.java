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
 * Created on Oct 29, 2007
 */

package com.bigdata.rdf.rules;

import java.io.Serializable;

import com.bigdata.rdf.spo.SPOAccessPath;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IChunkedIterator;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IBindingSet;
import com.bigdata.relation.rule.IConstraint;
import com.bigdata.relation.rule.IRule;
import com.bigdata.relation.rule.IRuleTaskFactory;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.Rule;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.ISolution;
import com.bigdata.relation.rule.eval.IStepTask;
import com.bigdata.relation.rule.eval.RuleStats;

/**
 * Base class for rules having a single predicate that is none bound in the tail
 * and a single variable in the head. These rules can be evaluated using
 * {@link SPOAccessPath#distinctTermScan()} rather than a full index scan. For
 * example:
 * 
 * <pre>
 *  rdf1:   (?u ?a ?y) -&gt; (?a rdf:type rdf:Property)
 *  rdfs4a: (?u ?a ?x) -&gt; (?u rdf:type rdfs:Resource)
 *  rdfs4b: (?u ?a ?v) -&gt; (?v rdf:type rdfs:Resource)
 * </pre>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractRuleDistinctTermScan extends Rule {
    
    /**
     * The sole unbound variable in the head of the rule.
     */
    private final IVariable<Long> h;

    /**
     * The access path that corresponds to the position of the unbound variable
     * reference from the head.
     */
    private final SPOKeyOrder keyOrder;
    
    private final IRuleTaskFactory taskFactory;

    public AbstractRuleDistinctTermScan(String name, SPOPredicate head,
            SPOPredicate[] body, IConstraint[] constraints) {

        super(name, head, body, constraints);
        
        // head must be one bound.
        assert head.getVariableCount() == 1;

        // tail must have one predicate.
        assert body.length == 1;
        
        // the predicate in the tail must be "none" bound.
        assert body[0].getVariableCount() == IRawTripleStore.N;

        // figure out which position in the head is the variable.
        if(head.s().isVar()) {
            
            h = (IVariable<Long>)head.s();
            
        } else if( head.p().isVar() ) {
            
            h = (IVariable<Long>)head.p();
            
        } else if( head.o().isVar() ) {
        
            h = (IVariable<Long>)head.o();
        
        } else {
            
            throw new AssertionError();
            
        }

        /*
         * figure out which access path we need for the distinct term scan which
         * will bind the variable in the head.
         */
        if (body[0].s() == h) {

            keyOrder = SPOKeyOrder.SPO;

        } else if (body[0].p() == h) {

            keyOrder = SPOKeyOrder.POS;

        } else if (body[0].o() == h) {

            keyOrder = SPOKeyOrder.OSP;

        } else {

            throw new AssertionError();
            
        }

        // @todo may serialize to much state?
        taskFactory = new IRuleTaskFactory() {

            public IStepTask newTask(IRule rule, IJoinNexus joinNexus,
                    IBuffer<ISolution> buffer) {

                return new DistinctTermScan(rule, joinNexus, buffer, h,
                        keyOrder);

            }
            
        };
        
    }
    
    public IRuleTaskFactory getTaskFactory() {
        
        return taskFactory;
        
    }
    
    /**
     * Selects the distinct term identifiers, substituting their binding in the
     * sole unbound variable in the head of the rule.
     */
    protected static class DistinctTermScan implements IStepTask, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -7570511260700545025L;
        
        private final IJoinNexus joinNexus;
        private final IBuffer<ISolution> buffer;
        
        private final IRule rule;
        
        /**
         * The sole unbound variable in the head of the rule.
         */
        private final IVariable<Long> h;

        /**
         * The access path that corresponds to the position of the unbound variable
         * reference from the head.
         */
        private final SPOKeyOrder keyOrder;
        
        /**
         * 
         * @param rule
         *            The rule (may have been specialized).
         * @param joinNexus
         * @param buffer
         *            The buffer on which the {@link ISolution}s will be
         *            written.
         * @param h
         *            The sole unbound variable in the head of the rule.
         * @param keyOrder
         *            The access path that corresponds to the position of the
         *            unbound variable reference from the head.
         */
        public DistinctTermScan(IRule rule, IJoinNexus joinNexus,
                IBuffer<ISolution> buffer, IVariable<Long> h,
                SPOKeyOrder keyOrder) {
        
            if (rule == null)
                throw new IllegalArgumentException();

            if (joinNexus == null)
                throw new IllegalArgumentException();

            if (buffer == null)
                throw new IllegalArgumentException();

            if (h == null)
                throw new IllegalArgumentException();
            
            if (keyOrder == null)
                throw new IllegalArgumentException();
            
            this.rule = rule;
            
            this.joinNexus = joinNexus;
            
            this.buffer = buffer;
            
            this.h = h;
            
            this.keyOrder = keyOrder;
            
        }
        
        public RuleStats call() {

            final long computeStart = System.currentTimeMillis();

            /*
             * find the distinct predicates in the KB (efficient op).
             */

            final long timestamp = joinNexus.getReadTimestamp();

            /*
             * Note: Since this task is always applied to a single tail rule,
             * the {@link TMUtility} rewrite of the rule will always read from
             * the focusStore alone. This makes the choice of the relation on
             * which to read easy - just read on whichever relation is specified
             * for tail[0].
             */
            final SPORelation relation = (SPORelation) joinNexus
                    .getIndexManager().getResourceLocator().locate(
                            rule.getHead().getRelationName(), timestamp);
            
//            final SPOAccessPath accessPath = relation.getAccessPath(
//                    keyOrder, rule.getTail(0));
            
//            IAccessPath accessPath = state.focusStore == null ? state.database
//                    .getAccessPath(keyOrder) : state.focusStore
//                    .getAccessPath(keyOrder);

            final RuleStats ruleStats = new RuleStats(rule);
            
            // there is only a single unbound variable for this rule.
            final IBindingSet bindingSet = new ArrayBindingSet(1/*capacity*/);

            final IChunkedIterator<Long> itr = relation.distinctTermScan(keyOrder); 
//                new DistinctTermScanner(
//                    relation.getExecutorService(), accessPath.getIndex()).iterator();
            
            try {

                while (itr.hasNext()) {

                    // Note: chunks are in ascending order since using scan on SPO index.
                    final Long[] chunk = itr.nextChunk();

                    ruleStats.chunkCount[0]++;

                    ruleStats.elementCount[0] += chunk.length;

                    for (Long id : chunk) {

                        // [id] is a distinct term identifier for the selected
                        // access path.

                        /*
                         * bind the unbound variable in the head of the rule.
                         * 
                         * Note: This explicitly leaves the other variables in
                         * the head unbound so that the justifications will be
                         * wildcards for those variables.
                         */

                        bindingSet.set(h, new Constant<Long>(id));

                        if (rule.isConsistent(bindingSet)) {

                            buffer.add(joinNexus.newSolution(rule, bindingSet));

                            ruleStats.solutionCount++;

                        }

                    }
                    
                }

            } finally {

                itr.close();

                ruleStats.elapsed += System.currentTimeMillis() - computeStart;

            }

            return ruleStats;
            
        }

    }

}
